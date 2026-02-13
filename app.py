"""
利用者情報 自動抽出・CSV出力ツール（プロトタイプ）
障害福祉サービス事業所向け - 受給者証・契約書の画像からデータを自動抽出
"""

import base64
import io
import json
import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# app.pyと同じディレクトリの.envを明示的に読み込む（システム環境変数より優先）
load_dotenv(Path(__file__).parent / ".env", override=True)


def get_secret(key: str) -> str:
    """Streamlit Cloud の secrets → .env の順でキーを取得"""
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return os.getenv(key, "")

# --- 定数定義 ---

CSV_COLUMNS = [
    "サービス利用拠点名",
    "利用者_姓",
    "利用者_名",
    "利用者_姓(ふりがな)",
    "利用者_名(ふりがな)",
    "性別",
    "生年月日 (YYYY年MM月DD日)",
    "障害福祉サービス受給者証番号",
    "支給決定開始日 (YYYY年MM月DD日)",
    "支給決定終了日 (YYYY年MM月DD日)",
    "モニタリング_月数",
    "モニタリング_終了月",
    "郵便番号",
    "都道府県",
    "住所",
]

EXTRACTION_PROMPT = """あなたは障害福祉サービスの書類読み取り専門のアシスタントです。
アップロードされた画像は「受給者証」または「契約書」です。

以下の項目を画像から読み取り、JSON形式で返してください。
読み取れない項目は空文字("")としてください。

各項目について、読み取りの確信度を "confidence" オブジェクトに記載してください。
- "high": はっきり読み取れた
- "low": 文字が不鮮明、推測が含まれる、または該当項目が書類上に見当たらないが推定した

抽出項目:
1. サービス利用拠点名
2. 利用者_姓
3. 利用者_名
4. 利用者_姓(ふりがな)
5. 利用者_名(ふりがな)
6. 性別 (男 or 女)
7. 生年月日 (YYYY年MM月DD日) 例: 1990年01月15日
8. 障害福祉サービス受給者証番号
9. 支給決定開始日 (YYYY年MM月DD日) 例: 2024年04月01日
10. 支給決定終了日 (YYYY年MM月DD日) 例: 2025年03月31日
11. モニタリング_月数
12. モニタリング_終了月
13. 郵便番号 (ハイフンなし7桁の数字のみ 例: 1234567)
14. 都道府県
15. 住所 (都道府県より後の部分)

回答はJSON形式のみで、余計な説明は不要です。以下の形式で返してください:
{
  "サービス利用拠点名": "",
  "利用者_姓": "",
  "利用者_名": "",
  "利用者_姓(ふりがな)": "",
  "利用者_名(ふりがな)": "",
  "性別": "",
  "生年月日 (YYYY年MM月DD日)": "",
  "障害福祉サービス受給者証番号": "",
  "支給決定開始日 (YYYY年MM月DD日)": "",
  "支給決定終了日 (YYYY年MM月DD日)": "",
  "モニタリング_月数": "",
  "モニタリング_終了月": "",
  "郵便番号": "",
  "都道府県": "",
  "住所": "",
  "confidence": {
    "サービス利用拠点名": "high",
    "利用者_姓": "high",
    "利用者_名": "high",
    "利用者_姓(ふりがな)": "low",
    "利用者_名(ふりがな)": "low",
    "性別": "high",
    "生年月日 (YYYY年MM月DD日)": "high",
    "障害福祉サービス受給者証番号": "high",
    "支給決定開始日 (YYYY年MM月DD日)": "high",
    "支給決定終了日 (YYYY年MM月DD日)": "high",
    "モニタリング_月数": "low",
    "モニタリング_終了月": "low",
    "郵便番号": "high",
    "都道府県": "high",
    "住所": "high"
  }
}
"""

# 必須項目（空欄だと信頼度が下がる重要フィールド）
REQUIRED_FIELDS = [
    "利用者_姓",
    "利用者_名",
    "生年月日 (YYYY年MM月DD日)",
    "障害福祉サービス受給者証番号",
    "支給決定開始日 (YYYY年MM月DD日)",
    "支給決定終了日 (YYYY年MM月DD日)",
]

# アップロード上限枚数
MAX_FILES = 100
# プレビューに表示する最大枚数
PREVIEW_MAX = 12
# バッチ処理の単位
BATCH_SIZE = 10


# --- ユーティリティ関数 ---


MAX_IMAGE_BYTES = 4_500_000  # base64変換後に5MB以内に収まるよう余裕を持たせる


def compress_image(image_bytes: bytes, media_type: str) -> tuple[bytes, str]:
    """画像がAPIの上限を超える場合にリサイズ・圧縮する"""
    from PIL import Image

    if len(image_bytes) <= MAX_IMAGE_BYTES:
        return image_bytes, media_type

    img = Image.open(io.BytesIO(image_bytes))

    # JPEG圧縮で縮小を試みる（品質を段階的に下げる）
    for quality in (85, 70, 50, 35):
        # 長辺が大きすぎる場合はリサイズ
        max_dim = 2048 if quality >= 70 else 1600
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)

        buf = io.BytesIO()
        rgb_img = img.convert("RGB") if img.mode != "RGB" else img
        rgb_img.save(buf, format="JPEG", quality=quality)
        result = buf.getvalue()
        if len(result) <= MAX_IMAGE_BYTES:
            return result, "image/jpeg"

    # 最終手段: さらに小さくリサイズ
    img.thumbnail((1200, 1200), Image.LANCZOS)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=30)
    return buf.getvalue(), "image/jpeg"


def encode_image_to_base64(image_bytes: bytes) -> str:
    """画像バイトデータをbase64文字列に変換"""
    return base64.b64encode(image_bytes).decode("utf-8")


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
            "Windows: https://github.com/oschwartz10612/poppler-windows/releases からダウンロード\n"
            "Mac: `brew install poppler`\n"
            "Linux: `sudo apt-get install poppler-utils`"
        )
    except Exception as e:
        st.error(f"PDF変換エラー: {e}")
    return None


def strip_postal_hyphen(val: str) -> str:
    """郵便番号からハイフンを除去して数字7桁のみにする"""
    digits = re.sub(r"[^\d]", "", val)
    return digits


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
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": image_base64,
                            },
                        },
                        {
                            "type": "text",
                            "text": EXTRACTION_PROMPT,
                        },
                    ],
                }
            ],
        )
        response_text = message.content[0].text
        result = parse_json_response(response_text)
        if result:
            # 郵便番号のハイフン除去を後処理でも保証
            postal = str(result.get("郵便番号", ""))
            if postal:
                result["郵便番号"] = strip_postal_hyphen(postal)
        return result
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
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{media_type};base64,{image_base64}",
                            },
                        },
                        {
                            "type": "text",
                            "text": EXTRACTION_PROMPT,
                        },
                    ],
                }
            ],
        )
        response_text = response.choices[0].message.content
        result = parse_json_response(response_text)
        if result:
            postal = str(result.get("郵便番号", ""))
            if postal:
                result["郵便番号"] = strip_postal_hyphen(postal)
        return result
    except Exception as e:
        st.error(f"OpenAI API エラー: {e}")
        return None


def parse_json_response(text: str) -> Optional[dict]:
    """APIレスポンスからJSONを抽出・パース"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # ```json ... ``` ブロックの抽出を試みる
    if "```" in text:
        start = text.find("```")
        end = text.rfind("```")
        if start != end:
            json_block = text[start:end]
            # ```json のプレフィックスを除去
            first_newline = json_block.find("\n")
            if first_newline != -1:
                json_block = json_block[first_newline + 1 :]
            try:
                return json.loads(json_block.strip())
            except json.JSONDecodeError:
                pass
    # { ... } を直接探す
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    st.error("AIからのレスポンスをJSON形式で解析できませんでした。")
    st.code(text, language="text")
    return None


def calc_confidence(data: dict) -> tuple[int, str, list[str]]:
    """
    レコードの照合率(%)・判定ラベル・要確認項目リストを返す。

    スコア計算:
    - 各項目に値があれば加点、空欄なら0点
    - AIが "high" と回答した項目は満点、"low" は半分
    - 必須項目は配点2倍
    """
    confidence_map = data.get("confidence", {})
    total_weight = 0
    earned = 0
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

    if pct >= 90 and not any(
        col in low_fields for col in REQUIRED_FIELDS
    ):
        label = "OK"
    elif pct >= 60:
        label = "要確認"
    else:
        label = "要確認(低)"

    return pct, label, low_fields


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
        confidence_info.append({
            "pct": pct,
            "label": label,
            "low_fields": low_fields,
        })
    display_cols = ["判定", "照合率"] + CSV_COLUMNS
    return pd.DataFrame(rows, columns=display_cols), confidence_info


def _match_key(row: dict) -> Optional[str]:
    """突合キーを生成。受給者証番号優先、なければ姓名+生年月日"""
    cert = str(row.get("障害福祉サービス受給者証番号", "")).strip()
    if cert:
        return f"cert:{cert}"
    sei = str(row.get("利用者_姓", "")).strip()
    mei = str(row.get("利用者_名", "")).strip()
    birth = str(row.get("生年月日 (YYYY年MM月DD日)", "")).strip()
    if sei and mei and birth:
        return f"name:{sei}|{mei}|{birth}"
    return None


def merge_records(data_list: list[dict]) -> list[dict]:
    """同一人物のレコードを突合し、空欄をできるだけ埋めたリストを返す"""
    from collections import OrderedDict

    groups: OrderedDict[str, dict] = OrderedDict()
    unmatched = []

    for data in data_list:
        key = _match_key(data)
        if key is None:
            unmatched.append(data)
            continue

        if key not in groups:
            merged = {col: data.get(col, "") for col in CSV_COLUMNS}
            # confidence情報もコピー
            merged["confidence"] = dict(data.get("confidence", {}))
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
                    # 両方ある場合: high優先、同じならより長い値を採用
                    if new_c == "high" and old_c == "low":
                        existing[col] = new_val
                        existing_conf[col] = "high"
                    elif len(new_val) > len(old_val) and new_c == old_c:
                        existing[col] = new_val
            existing["confidence"] = existing_conf

    return list(groups.values()) + unmatched


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """DataFrameをUTF-8 BOM付きCSVバイト列に変換"""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


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

    # ヘッダーが無い場合は1行目に追加
    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(CSV_COLUMNS, value_input_option="USER_ENTERED")

    # DataFrameの各行を追記
    rows = df.fillna("").astype(str).values.tolist()
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


# --- Streamlit UI ---


def inject_beforeunload_guard():
    """処理中にタブを閉じようとしたらブラウザのアラートを出すJS"""
    st.components.v1.html(
        """
        <script>
        // Streamlitの親windowにイベントを設定
        const win = window.parent || window;
        win.addEventListener('beforeunload', function(e) {
            e.preventDefault();
            e.returnValue = '';
        });
        </script>
        """,
        height=0,
    )


def remove_beforeunload_guard():
    """処理完了後にアラートを解除するJS"""
    st.components.v1.html(
        """
        <script>
        const win = window.parent || window;
        win.onbeforeunload = null;
        </script>
        """,
        height=0,
    )


def main():
    st.set_page_config(
        page_title="利用者情報 自動抽出ツール",
        page_icon="📋",
        layout="wide",
    )

    st.title("利用者情報 自動抽出・CSV出力ツール")
    st.caption("障害福祉サービス事業所向け — 受給者証・契約書からのデータ自動抽出プロトタイプ")

    # API選択（固定: Anthropic）
    api_provider = "Anthropic (Claude)"

    # 処理中フラグがあればタブ閉じアラートを有効化
    if st.session_state.get("processing"):
        inject_beforeunload_guard()

    # --- メインエリア ---

    # ステップ1: ファイルアップロード（複数対応・ドラッグ&ドロップ対応）
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
        return

    if len(uploaded_files) > MAX_FILES:
        st.error(f"アップロードは{MAX_FILES}枚までです。現在{len(uploaded_files)}枚選択されています。")
        return

    # アップロード直後はファイル名一覧のみ表示（画像データは読み込まない）
    file_names = [uf.name for uf in uploaded_files]
    st.success(f"{len(file_names)}件のファイルを検出しました。")

    # ステップ2: ファイル一覧（プレビューは折りたたみ内で遅延表示）
    st.header(f"② アップロードファイル一覧（{len(file_names)}件）")
    with st.expander("ファイル名一覧", expanded=False):
        for i, name in enumerate(file_names):
            st.text(f"{i+1}. {name}")

    with st.expander(f"画像プレビュー（先頭{PREVIEW_MAX}件）", expanded=False):
        preview_count = min(len(uploaded_files), PREVIEW_MAX)
        cols = st.columns(3)
        for i in range(preview_count):
            uf = uploaded_files[i]
            with cols[i % 3]:
                st.image(uf, caption=uf.name, use_container_width=True)

    # ステップ3: AI抽出（ボタン押下時に画像を読み込み・処理）
    st.header("③ AIによるデータ抽出")

    if st.button("すべてのデータを抽出する", type="primary", use_container_width=True):
        st.session_state["processing"] = True
        inject_beforeunload_guard()

        results = []
        file_conf_map = {}  # file_name -> confidence_pct
        images_for_review = []  # 信頼値低い写真用に保持
        progress = st.progress(0, text="準備中...")
        status_text = st.empty()

        total = len(uploaded_files)
        for i, uf in enumerate(uploaded_files):
            fname = uf.name
            batch_num = i // BATCH_SIZE + 1
            total_batches = (total - 1) // BATCH_SIZE + 1
            progress.progress(
                i / total,
                text=f"抽出中... ({i + 1}/{total}) バッチ {batch_num}/{total_batches}",
            )
            status_text.caption(f"処理中: {fname}")

            # ここで初めて画像データを読み込む（遅延読み込み）
            file_bytes = uf.read()
            is_pdf = fname.lower().endswith(".pdf")

            if is_pdf:
                image_bytes = convert_pdf_to_image(file_bytes)
                if image_bytes is None:
                    st.warning(f"PDF変換失敗: {fname}")
                    file_conf_map[fname] = 0
                    results.append({col: "" for col in CSV_COLUMNS})
                    continue
                mtype = "image/png"
            else:
                image_bytes = file_bytes
                ext = fname.lower().rsplit(".", 1)[-1]
                mtype = "image/jpeg" if ext in ("jpg", "jpeg") else "image/png"

            compressed, comp_mtype = compress_image(image_bytes, mtype)
            image_base64 = encode_image_to_base64(compressed)
            if api_provider == "Anthropic (Claude)":
                extracted = extract_with_anthropic(image_base64, comp_mtype)
            else:
                extracted = extract_with_openai(image_base64, comp_mtype)

            if extracted is not None:
                extracted["_source_file"] = fname
                results.append(extracted)
                pct, _, _ = calc_confidence(extracted)
                file_conf_map[fname] = pct
            else:
                st.warning(f"抽出失敗: {fname}")
                empty = {col: "" for col in CSV_COLUMNS}
                empty["_source_file"] = fname
                results.append(empty)
                file_conf_map[fname] = 0

            # 信頼値が低い写真は後で表示するために保持
            if file_conf_map[fname] < 90:
                images_for_review.append((fname, image_bytes))

        progress.progress(1.0, text=f"完了！ {len(results)}件を抽出しました。")
        status_text.empty()

        # 同一人物のレコードを突合
        merged = merge_records(results)
        st.session_state["extracted_data"] = merged
        st.session_state["raw_count"] = len(results)
        st.session_state["file_conf_map"] = file_conf_map
        st.session_state["images_for_review"] = images_for_review
        st.session_state["processing"] = False
        st.rerun()

    # ステップ3.5: 信頼値が低い写真のハイライト表示
    if "images_for_review" in st.session_state and "file_conf_map" in st.session_state:
        file_conf_map = st.session_state["file_conf_map"]
        low_conf_images = st.session_state["images_for_review"]
        if low_conf_images:
            st.header(f"⚠ 読取精度が低い書類（{len(low_conf_images)}件）")
            st.caption("以下の書類は読み取り信頼度が低いため、抽出結果を重点的に確認してください。")
            cols = st.columns(min(len(low_conf_images), 3))
            for i, (fname, img_bytes) in enumerate(low_conf_images):
                pct = file_conf_map.get(fname, 0)
                with cols[i % 3]:
                    st.image(img_bytes, use_container_width=True)
                    if pct < 60:
                        st.error(f"📄 {fname}  —  照合率 **{pct}%**")
                    else:
                        st.warning(f"📄 {fname}  —  照合率 **{pct}%**")

    # ステップ4: 結果確認・編集
    if "extracted_data" in st.session_state:
        data_list = st.session_state["extracted_data"]
        raw_count = st.session_state.get("raw_count", len(data_list))

        df, conf_info = build_dataframe(data_list)
        ok_count = sum(1 for c in conf_info if c["label"] == "OK")
        review_count = len(conf_info) - ok_count

        if raw_count != len(data_list):
            st.header(f"④ 抽出結果の確認・編集（{raw_count}件 → 突合後 {len(data_list)}件）")
        else:
            st.header(f"④ 抽出結果の確認・編集（{len(data_list)}件）")

        # サマリー表示
        col1, col2, col3 = st.columns(3)
        col1.metric("OK (確認不要)", f"{ok_count}件")
        col2.metric("要確認", f"{review_count}件")
        col3.metric("平均照合率", f"{sum(c['pct'] for c in conf_info) // len(conf_info)}%")

        # フィルタ
        view_filter = st.radio(
            "表示フィルタ",
            ["すべて", "要確認のみ", "OKのみ"],
            horizontal=True,
        )

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

        # 要確認項目の詳細表示
        if display_conf and any(c["low_fields"] for c in display_conf):
            with st.expander("要確認項目の詳細", expanded=review_count > 0):
                for i, c in enumerate(display_conf):
                    if c["low_fields"]:
                        name = display_df.iloc[i].get("利用者_姓", "") + " " + display_df.iloc[i].get("利用者_名", "")
                        name = name.strip() or f"行{i+1}"
                        st.markdown(
                            f"**{name}** (照合率 {c['pct']}%) — "
                            f"不明項目: {', '.join(c['low_fields'])}"
                        )

        st.caption("各セルをクリックして直接修正できます。「判定」「照合率」列は出力に含まれません。")
        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            num_rows="dynamic",
            disabled=["判定", "照合率"],
            key="data_editor",
        )

        # CSV出力用にはデータ列のみ抽出
        export_df = edited_df[CSV_COLUMNS] if all(c in edited_df.columns for c in CSV_COLUMNS) else edited_df

        # ステップ5: CSVダウンロード
        st.header("⑤ CSVダウンロード")
        csv_bytes = to_csv_bytes(export_df)
        st.download_button(
            label=f"CSVファイルをダウンロード（{len(export_df)}件）",
            data=csv_bytes,
            file_name="利用者情報.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # ステップ5': スプレッドシートへ反映（PoC）
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

        # 処理完了後のアラート解除
        remove_beforeunload_guard()


if __name__ == "__main__":
    main()
