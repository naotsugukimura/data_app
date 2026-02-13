"""
利用者情報 自動抽出・CSV出力ツール（プロトタイプ）
障害福祉サービス事業所向け - 受給者証・契約書の画像からデータを自動抽出
"""

import base64
import io
import json
import os
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# app.pyと同じディレクトリの.envを明示的に読み込む（システム環境変数より優先）
load_dotenv(Path(__file__).parent / ".env", override=True)

# =============================================================================
# 定数
# =============================================================================

CSV_COLUMNS = [
    "事業",
    "利用者_姓",
    "利用者_名",
    "利用者_せい",
    "利用者_めい",
    "性別",
    "生年月日",
    "保護者_姓",
    "保護者_名",
    "契約日",
    "受給者証タイプ",
    "受給者証番号",
    "支給決定開始日",
    "支給決定満了日",
    "モニタリング_当初Nか月",
    "モニタリング_満了月からNか月ごと",
    "郵便番号",
    "都道府県",
    "住所",
    "電話番号",
    "メールアドレス",
]

REQUIRED_FIELDS = [
    "事業",
    "利用者_姓",
    "利用者_名",
    "利用者_せい",
    "利用者_めい",
    "性別",
    "契約日",
    "受給者証タイプ",
    "受給者証番号",
    "支給決定開始日",
    "支給決定満了日",
    "モニタリング_当初Nか月",
    "モニタリング_満了月からNか月ごと",
]

EXTRACTION_PROMPT = """あなたは障害福祉サービスの書類読み取り専門のアシスタントです。
アップロードされた画像は「受給者証」または「契約書」です。

以下の項目を画像から読み取り、JSON形式で返してください。
読み取れない項目は空文字("")としてください。

各項目について、読み取りの確信度を "confidence" オブジェクトに記載してください。
- "high": はっきり読み取れた
- "low": 文字が不鮮明、推測が含まれる、または該当項目が書類上に見当たらないが推定した

抽出項目:
1. 事業 — サービス種別。「計画相談支援」「障害児相談支援」など
2. 利用者_姓
3. 利用者_名
4. 利用者_せい — 姓のひらがな読み
5. 利用者_めい — 名のひらがな読み
6. 性別 — 男 or 女
7. 生年月日 — 例: 1990年01月15日
8. 保護者_姓 — 児童の場合の保護者の姓（該当なければ空文字）
9. 保護者_名 — 児童の場合の保護者の名（該当なければ空文字）
10. 契約日 — 利用開始日。例: 2020年08月01日
11. 受給者証タイプ — 「障がい福祉サービス受給者証」「地域相談支援受給者証」「障がい児通所受給者証」のいずれか
12. 受給者証番号 — 10桁の数字
13. 支給決定開始日 — 例: 2023年02月05日
14. 支給決定満了日 — 例: 2024年02月29日
15. モニタリング_当初Nか月 — 数字のみ（例: 3）
16. モニタリング_満了月からNか月ごと — 数字のみ（例: 6）
17. 郵便番号 — ハイフンなし7桁の数字のみ（例: 8120011）
18. 都道府県
19. 住所 — 都道府県より後の部分
20. 電話番号 — ハイフン付きでOK（例: 092-710-4570）
21. メールアドレス

回答はJSON形式のみで、余計な説明は不要です。以下の形式で返してください:
{
  "事業": "",
  "利用者_姓": "",
  "利用者_名": "",
  "利用者_せい": "",
  "利用者_めい": "",
  "性別": "",
  "生年月日": "",
  "保護者_姓": "",
  "保護者_名": "",
  "契約日": "",
  "受給者証タイプ": "",
  "受給者証番号": "",
  "支給決定開始日": "",
  "支給決定満了日": "",
  "モニタリング_当初Nか月": "",
  "モニタリング_満了月からNか月ごと": "",
  "郵便番号": "",
  "都道府県": "",
  "住所": "",
  "電話番号": "",
  "メールアドレス": "",
  "confidence": {
    "事業": "high",
    "利用者_姓": "high",
    "利用者_名": "high",
    "利用者_せい": "low",
    "利用者_めい": "low",
    "性別": "high",
    "生年月日": "high",
    "保護者_姓": "low",
    "保護者_名": "low",
    "契約日": "high",
    "受給者証タイプ": "high",
    "受給者証番号": "high",
    "支給決定開始日": "high",
    "支給決定満了日": "high",
    "モニタリング_当初Nか月": "low",
    "モニタリング_満了月からNか月ごと": "low",
    "郵便番号": "high",
    "都道府県": "high",
    "住所": "high",
    "電話番号": "low",
    "メールアドレス": "low"
  }
}
"""

MAX_FILES = 10
MAX_IMAGE_BYTES = 4_500_000  # base64変換後に5MB以内に収まるよう余裕を持たせる

# =============================================================================
# シークレット取得
# =============================================================================


def get_secret(key: str) -> str:
    """Streamlit Cloud の secrets → .env の順でキーを取得"""
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return os.getenv(key, "")


# =============================================================================
# 画像処理
# =============================================================================


def compress_image(image_bytes: bytes, media_type: str) -> tuple[bytes, str]:
    """画像がAPIの上限を超える場合にリサイズ・圧縮する"""
    from PIL import Image

    if len(image_bytes) <= MAX_IMAGE_BYTES:
        return image_bytes, media_type

    img = Image.open(io.BytesIO(image_bytes))

    for quality in (85, 70, 50, 35):
        max_dim = 2048 if quality >= 70 else 1600
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)

        buf = io.BytesIO()
        rgb_img = img.convert("RGB") if img.mode != "RGB" else img
        rgb_img.save(buf, format="JPEG", quality=quality)
        result = buf.getvalue()
        if len(result) <= MAX_IMAGE_BYTES:
            return result, "image/jpeg"

    img.thumbnail((1200, 1200), Image.LANCZOS)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=30)
    return buf.getvalue(), "image/jpeg"


def convert_pdf_to_image(pdf_bytes: bytes) -> Optional[bytes]:
    """PDFの1ページ目を画像に変換"""
    try:
        from pdf2image import convert_from_bytes

        images = convert_from_bytes(pdf_bytes, first_page=1, last_page=1, dpi=200)
        if images:
            buf = io.BytesIO()
            images[0].save(buf, format="PNG")
            return buf.getvalue()
    except ImportError:
        st.error(
            "pdf2imageがインストールされていません。"
            "また、Popplerのインストールも必要です。\n"
            "Windows: https://github.com/oschwartz10612/poppler-windows/releases\n"
            "Mac: `brew install poppler`\n"
            "Linux: `sudo apt-get install poppler-utils`"
        )
    except Exception as e:
        st.error(f"PDF変換エラー: {e}")
    return None


def get_media_type(filename: str) -> str:
    """ファイル名から MIME タイプを判定"""
    ext = filename.lower().rsplit(".", 1)[-1]
    return "image/jpeg" if ext in ("jpg", "jpeg") else "image/png"


# =============================================================================
# AI 抽出
# =============================================================================


def parse_json_response(text: str) -> Optional[dict]:
    """APIレスポンスからJSONを抽出・パース"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    if "```" in text:
        start = text.find("```")
        end = text.rfind("```")
        if start != end:
            json_block = text[start:end]
            first_newline = json_block.find("\n")
            if first_newline != -1:
                json_block = json_block[first_newline + 1:]
            try:
                return json.loads(json_block.strip())
            except json.JSONDecodeError:
                pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass

    st.error("AIからのレスポンスをJSON形式で解析できませんでした。")
    st.code(text, language="text")
    return None


def _postprocess_extraction(result: dict) -> dict:
    """抽出結果の後処理（郵便番号ハイフン除去など）"""
    postal = str(result.get("郵便番号", ""))
    if postal:
        result["郵便番号"] = re.sub(r"[^\d]", "", postal)
    return result


def extract_with_anthropic(image_base64: str, media_type: str) -> Optional[dict]:
    """Anthropic Claude Vision APIで画像からデータを抽出"""
    try:
        import anthropic

        api_key = get_secret("ANTHROPIC_API_KEY")
        if not api_key or api_key.startswith("sk-ant-xxx"):
            st.error("ANTHROPIC_API_KEYが設定されていません。.envファイルまたはStreamlit Secretsを確認してください。")
            return None

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_base64}},
                    {"type": "text", "text": EXTRACTION_PROMPT},
                ],
            }],
        )
        result = parse_json_response(message.content[0].text)
        return _postprocess_extraction(result) if result else None
    except Exception as e:
        st.error(f"Anthropic API エラー: {e}")
        return None


def extract_with_openai(image_base64: str, media_type: str) -> Optional[dict]:
    """OpenAI GPT-4 Vision APIで画像からデータを抽出"""
    try:
        from openai import OpenAI

        api_key = get_secret("OPENAI_API_KEY")
        if not api_key or api_key.startswith("sk-xxx"):
            st.error("OPENAI_API_KEYが設定されていません。.envファイルまたはStreamlit Secretsを確認してください。")
            return None

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{image_base64}"}},
                    {"type": "text", "text": EXTRACTION_PROMPT},
                ],
            }],
        )
        result = parse_json_response(response.choices[0].message.content)
        return _postprocess_extraction(result) if result else None
    except Exception as e:
        st.error(f"OpenAI API エラー: {e}")
        return None


# =============================================================================
# データ処理（信頼度・突合・DataFrame構築）
# =============================================================================


def calc_confidence(data: dict) -> tuple[int, str, list[str]]:
    """レコードの照合率(%)・判定ラベル・要確認項目リストを返す"""
    confidence_map = data.get("confidence", {})
    total_weight = 0
    earned = 0.0
    low_fields = []

    for col in CSV_COLUMNS:
        weight = 2 if col in REQUIRED_FIELDS else 1
        total_weight += weight
        val = str(data.get(col, "")).strip()
        ai_conf = confidence_map.get(col, "high" if val else "low")

        if not val:
            low_fields.append(col)
        elif ai_conf == "low":
            earned += weight * 0.5
            low_fields.append(col)
        else:
            earned += weight

    pct = int(earned / total_weight * 100) if total_weight else 0

    if pct >= 90 and not any(col in low_fields for col in REQUIRED_FIELDS):
        label = "OK"
    elif pct >= 60:
        label = "要確認"
    else:
        label = "要確認(低)"

    return pct, label, low_fields


def is_record_ok(pct: int, low_fields: list[str]) -> bool:
    """照合率90%以上かつ必須項目に問題なしならTrue"""
    return pct >= 90 and not any(col in low_fields for col in REQUIRED_FIELDS)


def _match_key(row: dict) -> Optional[str]:
    """突合キーを生成。受給者証番号優先、なければ姓名+生年月日"""
    cert = str(row.get("受給者証番号", "")).strip()
    if cert:
        return f"cert:{cert}"
    sei = str(row.get("利用者_姓", "")).strip()
    mei = str(row.get("利用者_名", "")).strip()
    birth = str(row.get("生年月日", "")).strip()
    if sei and mei and birth:
        return f"name:{sei}|{mei}|{birth}"
    return None


def merge_records(data_list: list[dict]) -> list[dict]:
    """同一人物のレコードを突合し、空欄をできるだけ埋めたリストを返す"""
    groups: OrderedDict[str, dict] = OrderedDict()
    unmatched = []

    for data in data_list:
        key = _match_key(data)
        if key is None:
            unmatched.append(data)
            continue

        if key not in groups:
            merged = {col: data.get(col, "") for col in CSV_COLUMNS}
            merged["confidence"] = dict(data.get("confidence", {}))
            merged["_source_files"] = [data.get("_source_file", "")]
            groups[key] = merged
        else:
            existing = groups[key]
            existing_conf = existing.get("confidence", {})
            new_conf = data.get("confidence", {})
            for col in CSV_COLUMNS:
                new_val = str(data.get(col, "")).strip()
                old_val = str(existing.get(col, "")).strip()
                new_c = new_conf.get(col, "low")
                old_c = existing_conf.get(col, "low")
                if not old_val and new_val:
                    existing[col] = new_val
                    existing_conf[col] = new_c
                elif old_val and new_val:
                    if new_c == "high" and old_c == "low":
                        existing[col] = new_val
                        existing_conf[col] = "high"
                    elif len(new_val) > len(old_val) and new_c == old_c:
                        existing[col] = new_val
            existing["confidence"] = existing_conf
            src = data.get("_source_file", "")
            if src and src not in existing.get("_source_files", []):
                existing.setdefault("_source_files", []).append(src)

    return list(groups.values()) + unmatched


def build_dataframe(data_list: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    """抽出データのリストからDataFrame+信頼度情報を構築"""
    rows = []
    confidence_info = []
    for data in data_list:
        pct, label, low_fields = calc_confidence(data)
        row = {"判定": label, "照合率": f"{pct}%"}
        for col in CSV_COLUMNS:
            row[col] = data.get(col, "")
        rows.append(row)
        confidence_info.append({"pct": pct, "label": label, "low_fields": low_fields})
    display_cols = ["判定", "照合率"] + CSV_COLUMNS
    return pd.DataFrame(rows, columns=display_cols), confidence_info


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """DataFrameをUTF-8 BOM付きCSVバイト列に変換"""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


# =============================================================================
# Google スプレッドシート連携
# =============================================================================


def append_to_google_sheet(df: pd.DataFrame, spreadsheet_url: str, sheet_name: str) -> int:
    """Google スプレッドシートの末尾にデータを追記し、追記した行数を返す"""
    import gspread

    creds_path = Path(__file__).parent / "credentials.json"
    if not creds_path.exists():
        raise FileNotFoundError(
            "credentials.json が見つかりません。"
            "Google Cloud のサービスアカウントキーをダウンロードして "
            f"{creds_path} に配置してください。"
        )

    gc = gspread.service_account(filename=str(creds_path))
    sh = gc.open_by_url(spreadsheet_url)
    worksheet = sh.worksheet(sheet_name)

    if not worksheet.get_all_values():
        worksheet.append_row(CSV_COLUMNS, value_input_option="USER_ENTERED")

    rows = df.fillna("").astype(str).values.tolist()
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


# =============================================================================
# タブ閉じ防止 JS
# =============================================================================

_GUARD_ON_JS = """
<script>
(function() {
    try { var w = window.parent.document ? window.parent : window; }
    catch(e) { var w = window; }
    function guard(e) { e.preventDefault(); e.returnValue = '処理中です。本当にページを離れますか？'; return e.returnValue; }
    w.addEventListener('beforeunload', guard);
    w.__streamlit_guard = guard;
})();
</script>
"""

_GUARD_OFF_JS = """
<script>
(function() {
    try { var w = window.parent.document ? window.parent : window; }
    catch(e) { var w = window; }
    if (w.__streamlit_guard) { w.removeEventListener('beforeunload', w.__streamlit_guard); delete w.__streamlit_guard; }
    w.onbeforeunload = null;
})();
</script>
"""


def inject_beforeunload_guard():
    st.components.v1.html(_GUARD_ON_JS, height=0)


def remove_beforeunload_guard():
    st.components.v1.html(_GUARD_OFF_JS, height=0)


# =============================================================================
# UI: ① アップロード
# =============================================================================


def render_upload_section() -> list:
    """ファイルアップロードUIを描画し、アップロードされたファイルリストを返す"""
    st.header("① 書類をアップロード")
    st.caption(f"ファイルをドラッグ&ドロップ、またはクリックして選択（最大{MAX_FILES}枚まで）")

    uploaded_files = st.file_uploader(
        "受給者証・契約書の画像またはPDFをアップロード",
        type=["jpg", "jpeg", "png", "pdf"],
        accept_multiple_files=True,
        label_visibility="collapsed",
    )

    if not uploaded_files:
        st.info("ファイルをドラッグ&ドロップ、またはクリックしてアップロードしてください。")
        return []

    if len(uploaded_files) > MAX_FILES:
        st.error(f"アップロードは{MAX_FILES}枚までです。現在{len(uploaded_files)}枚選択されています。")
        return []

    return uploaded_files


# =============================================================================
# UI: ② ファイル一覧
# =============================================================================


def render_file_list(uploaded_files: list):
    """アップロード済みファイルの一覧とプレビューを描画"""
    n = len(uploaded_files)
    st.header(f"② アップロードファイル一覧（{n}件）")
    st.success(f"{n}件のファイルを検出しました。")

    with st.expander("ファイル名一覧", expanded=False):
        for i, uf in enumerate(uploaded_files):
            st.text(f"{i + 1}. {uf.name}")

    with st.expander("画像プレビュー", expanded=False):
        cols = st.columns(3)
        for i, uf in enumerate(uploaded_files):
            with cols[i % 3]:
                st.image(uf, caption=uf.name, use_container_width=True)


# =============================================================================
# UI: ③ AI抽出
# =============================================================================


def _extract_single_file(uf) -> tuple[Optional[dict], bytes]:
    """1ファイルの画像読み込み→圧縮→API抽出を行い (抽出結果, 画像bytes) を返す"""
    file_bytes = uf.read()
    fname = uf.name

    if fname.lower().endswith(".pdf"):
        image_bytes = convert_pdf_to_image(file_bytes)
        if image_bytes is None:
            st.warning(f"PDF変換失敗: {fname}")
            return None, file_bytes
        mtype = "image/png"
    else:
        image_bytes = file_bytes
        mtype = get_media_type(fname)

    compressed, comp_mtype = compress_image(image_bytes, mtype)
    image_base64 = base64.b64encode(compressed).decode("utf-8")
    extracted = extract_with_anthropic(image_base64, comp_mtype)

    return extracted, image_bytes


def render_extraction_section(uploaded_files: list):
    """AI抽出のボタンと処理ロジック"""
    st.header("③ AIによるデータ抽出")

    if not st.button("すべてのデータを抽出する", type="primary", use_container_width=True):
        return

    st.session_state["processing"] = True
    inject_beforeunload_guard()

    results = []
    all_images = {}
    total = len(uploaded_files)
    progress = st.progress(0, text=f"抽出中... 0/{total}件 完了")
    start_time = time.time()

    for i, uf in enumerate(uploaded_files):
        # 残り時間の推定
        if i > 0:
            elapsed = time.time() - start_time
            avg_sec = elapsed / i
            remaining = avg_sec * (total - i)
            if remaining >= 60:
                eta = f"（残り約{int(remaining // 60)}分{int(remaining % 60)}秒）"
            else:
                eta = f"（残り約{int(remaining)}秒）"
        else:
            eta = ""
        progress.progress(i / total, text=f"抽出中... {i}/{total}件 完了 {eta}")

        extracted, image_bytes = _extract_single_file(uf)
        fname = uf.name

        if extracted is not None:
            extracted["_source_file"] = fname
            results.append(extracted)
        else:
            if not fname.lower().endswith(".pdf"):
                st.warning(f"抽出失敗: {fname}")
            empty = {col: "" for col in CSV_COLUMNS}
            empty["_source_file"] = fname
            results.append(empty)

        all_images[fname] = image_bytes

    elapsed_total = time.time() - start_time
    if elapsed_total >= 60:
        time_str = f"{int(elapsed_total // 60)}分{int(elapsed_total % 60)}秒"
    else:
        time_str = f"{int(elapsed_total)}秒"
    progress.progress(1.0, text=f"完了！ {total}/{total}件を{time_str}で抽出しました。")

    merged = merge_records(results)
    st.session_state["extracted_data"] = merged
    st.session_state["raw_count"] = len(results)
    st.session_state["all_images"] = all_images
    st.session_state["processing"] = False
    st.rerun()


# =============================================================================
# UI: 画像付きレコード確認・編集
# =============================================================================


def _get_record_name(data: dict, data_idx: int) -> str:
    """レコードの表示名を取得"""
    name = f"{data.get('利用者_姓', '')} {data.get('利用者_名', '')}".strip()
    return name or f"レコード{data_idx + 1}"


def _render_review_card(
    item_idx: int, data_idx: int, data: dict,
    imgs: list, pct: int, low_fields: list[str],
    delete_checks: dict,
):
    """レビューカード1件を描画"""
    name = _get_record_name(data, data_idx)
    merged_label = f"（書類{len(imgs)}枚を突合）" if len(imgs) > 1 else ""

    if pct < 60:
        st.error(f"**{name}** — 照合率 {pct}%{merged_label}　不明項目: {', '.join(low_fields)}")
    elif pct < 90:
        st.warning(f"**{name}** — 照合率 {pct}%{merged_label}　不明項目: {', '.join(low_fields)}")
    else:
        st.success(f"**{name}** — 照合率 {pct}%{merged_label}")

    col_img, col_form = st.columns([1, 2])

    with col_img:
        if len(imgs) > 1:
            img_tabs = st.tabs([f"書類{i + 1}" for i in range(len(imgs))])
            for i, (fname, img_bytes) in enumerate(imgs):
                with img_tabs[i]:
                    st.image(img_bytes, caption=fname, use_container_width=True)
        else:
            st.image(imgs[0][1], caption=imgs[0][0], use_container_width=True)

    with col_form:
        form_cols = st.columns(3)
        for fi, col_name in enumerate(CSV_COLUMNS):
            with form_cols[fi % 3]:
                is_low = col_name in low_fields
                label_suffix = " ⚠" if is_low else ""
                st.text_input(
                    f"{col_name}{label_suffix}",
                    value=str(data.get(col_name, "")),
                    key=f"review_{item_idx}_{col_name}",
                )

        delete_checks[item_idx] = st.checkbox(
            "このレコードを削除する",
            key=f"del_check_{item_idx}",
        )

    st.divider()


def _apply_review_changes(review_items: list, delete_checks: dict, data_list: list):
    """レビュー結果を data_list に反映し、削除レコードを除去する"""
    del_indices = set()
    deleted_files = set()

    for item_idx, (data_idx, _data, imgs, _pct, _lf) in enumerate(review_items):
        if delete_checks.get(item_idx):
            del_indices.add(data_idx)
            deleted_files.update(f for f, _ in imgs)
        else:
            for col_name in CSV_COLUMNS:
                val = st.session_state.get(f"review_{item_idx}_{col_name}", "")
                data_list[data_idx][col_name] = val
            if "confidence" in data_list[data_idx]:
                for col_name in CSV_COLUMNS:
                    data_list[data_idx]["confidence"][col_name] = "high"

    if del_indices:
        for idx in sorted(del_indices, reverse=True):
            data_list.pop(idx)
        for fn in deleted_files:
            st.session_state["all_images"].pop(fn, None)

    st.session_state["extracted_data"] = data_list

    applied = len(review_items) - len(del_indices)
    msg_parts = []
    if applied:
        msg_parts.append(f"{applied}件を修正反映")
    if del_indices:
        msg_parts.append(f"{len(del_indices)}件を削除")
    st.success("・".join(msg_parts) + " しました。")
    st.rerun()


def render_review_section():
    """画像付きレコード確認・編集UI"""
    if "all_images" not in st.session_state or "extracted_data" not in st.session_state:
        return

    img_map = st.session_state["all_images"]
    data_list = st.session_state["extracted_data"]

    review_items = []
    for idx, data in enumerate(data_list):
        src_files = data.get("_source_files", [data.get("_source_file", "")])
        matching_imgs = [(f, img_map[f]) for f in src_files if f in img_map]
        if matching_imgs:
            pct, _label, low_fields = calc_confidence(data)
            review_items.append((idx, data, matching_imgs, pct, low_fields))

    if not review_items:
        return

    items_ok = [(ii, di, d, im, p, lf)
                for ii, (di, d, im, p, lf) in enumerate(review_items)
                if is_record_ok(p, lf)]
    items_ng = [(ii, di, d, im, p, lf)
                for ii, (di, d, im, p, lf) in enumerate(review_items)
                if not is_record_ok(p, lf)]

    st.header("画像付きレコード確認")
    tab_ng, tab_ok = st.tabs([
        f"要確認（{len(items_ng)}件）",
        f"OK（{len(items_ok)}件）",
    ])

    delete_checks: dict[int, bool] = {}

    with tab_ng:
        if items_ng:
            st.caption("修正・削除選択した後、下部の「すべての修正をまとめて反映」で一括反映されます。")
            for item_idx, data_idx, data, imgs, pct, low_fields in items_ng:
                _render_review_card(item_idx, data_idx, data, imgs, pct, low_fields, delete_checks)
        else:
            st.info("要確認レコードはありません。")

    with tab_ok:
        if items_ok:
            st.caption("照合率が高いレコードです。内容に問題がなければそのままでOKです。")
            for item_idx, data_idx, data, imgs, pct, low_fields in items_ok:
                _render_review_card(item_idx, data_idx, data, imgs, pct, low_fields, delete_checks)
        else:
            st.info("照合率OKのレコードはありません。")

    if st.button("すべての修正をまとめて反映", type="primary", use_container_width=True):
        _apply_review_changes(review_items, delete_checks, data_list)


# =============================================================================
# UI: ④ 結果確認・編集 / ⑤ エクスポート
# =============================================================================


def render_results_section():
    """抽出結果テーブルの表示・編集・CSV/スプレッドシートエクスポート"""
    if "extracted_data" not in st.session_state:
        return

    data_list = st.session_state["extracted_data"]
    raw_count = st.session_state.get("raw_count", len(data_list))

    df, conf_info = build_dataframe(data_list)
    ok_count = sum(1 for c in conf_info if c["label"] == "OK")
    review_count = len(conf_info) - ok_count

    # ヘッダー
    if raw_count != len(data_list):
        st.header(f"④ 抽出結果の確認・編集（{raw_count}件 → 突合後 {len(data_list)}件）")
    else:
        st.header(f"④ 抽出結果の確認・編集（{len(data_list)}件）")

    # サマリー
    col1, col2, col3 = st.columns(3)
    col1.metric("OK (確認不要)", f"{ok_count}件")
    col2.metric("要確認", f"{review_count}件")
    avg_pct = sum(c["pct"] for c in conf_info) // len(conf_info) if conf_info else 0
    col3.metric("平均照合率", f"{avg_pct}%")

    # フィルタ
    view_filter = st.radio("表示フィルタ", ["すべて", "要確認のみ", "OKのみ"], horizontal=True)

    if view_filter == "要確認のみ":
        mask = df["判定"] != "OK"
        display_df = df[mask].reset_index(drop=True)
        display_conf = [c for c in conf_info if c["label"] != "OK"]
    elif view_filter == "OKのみ":
        mask = df["判定"] == "OK"
        display_df = df[mask].reset_index(drop=True)
        display_conf = [c for c in conf_info if c["label"] == "OK"]
    else:
        display_df = df
        display_conf = conf_info

    # 要確認項目の詳細
    if display_conf and any(c["low_fields"] for c in display_conf):
        with st.expander("要確認項目の詳細", expanded=review_count > 0):
            for i, c in enumerate(display_conf):
                if c["low_fields"]:
                    row = display_df.iloc[i]
                    name = f"{row.get('利用者_姓', '')} {row.get('利用者_名', '')}".strip() or f"行{i + 1}"
                    st.markdown(f"**{name}** (照合率 {c['pct']}%) — 不明項目: {', '.join(c['low_fields'])}")

    # 編集可能テーブル
    st.caption("各セルをクリックして直接修正できます。「判定」「照合率」列は出力に含まれません。")
    edited_df = st.data_editor(
        display_df,
        use_container_width=True,
        num_rows="dynamic",
        disabled=["判定", "照合率"],
        key="data_editor",
    )

    export_df = edited_df[CSV_COLUMNS] if all(c in edited_df.columns for c in CSV_COLUMNS) else edited_df

    # ⑤ CSVダウンロード
    st.header("⑤ CSVダウンロード")
    st.download_button(
        label=f"CSVファイルをダウンロード（{len(export_df)}件）",
        data=to_csv_bytes(export_df),
        file_name="利用者情報.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # ⑤' スプレッドシート連携
    st.header("⑤' スプレッドシートへ反映")

    spreadsheet_url = st.text_input(
        "Google スプレッドシートのURL",
        value=st.session_state.get("spreadsheet_url", ""),
        placeholder="https://docs.google.com/spreadsheets/d/xxxxx/edit",
    )
    sheet_name = st.text_input(
        "シート名",
        value=st.session_state.get("sheet_name", "シート1"),
    )
    st.session_state["spreadsheet_url"] = spreadsheet_url
    st.session_state["sheet_name"] = sheet_name

    if st.button(
        f"スプレッドシートに追記する（{len(export_df)}件）",
        type="primary",
        use_container_width=True,
        disabled=not spreadsheet_url,
    ):
        try:
            with st.spinner("スプレッドシートに書き込み中..."):
                count = append_to_google_sheet(export_df, spreadsheet_url, sheet_name)
            st.success(f"{count}件のデータをスプレッドシートに追記しました。")
        except FileNotFoundError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"スプレッドシート書き込みエラー: {e}")

    remove_beforeunload_guard()


# =============================================================================
# メイン
# =============================================================================


def main():
    st.set_page_config(page_title="利用者情報 自動抽出ツール", page_icon="📋", layout="wide")
    st.title("利用者情報 自動抽出・CSV出力ツール")
    st.caption("障害福祉サービス事業所向け — 受給者証・契約書からのデータ自動抽出プロトタイプ")

    if st.session_state.get("processing"):
        inject_beforeunload_guard()

    # ① アップロード
    uploaded_files = render_upload_section()
    if not uploaded_files:
        return

    # ② ファイル一覧
    render_file_list(uploaded_files)

    # ③ AI抽出
    render_extraction_section(uploaded_files)

    # 画像付きレコード確認・編集
    render_review_section()

    # ④⑤ 結果テーブル・エクスポート
    render_results_section()


if __name__ == "__main__":
    main()
