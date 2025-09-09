# -*- coding: utf-8 -*-
import streamlit as st, pandas as pd, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import urllib.parse as up
from urllib.parse import urlparse
import re, math

# ---------------- Google auth ----------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

@st.cache_resource
def get_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )
    return gspread.authorize(creds)

# ---------------- Friendly names for sheet IDs (edit labels as you like) ----------------
SHEET_NAME_MAP = {
    "1a8hov55dVo4X4wJGMrdjVjIVYaUr8zLKDQSZ68QP-JU": "Core Policies",
    "1Z192qqZpw498AnN6LRAjYjy2_1wOkoXHDSUU2Xiejo8": "Quality Metrics",
    "1gThht-uDroRnnyQwuu1hRwc1OEEvlkCslp_bCNA5gSw": "Infection Control",
    "1XHpnubxzplhXHgXTSyATDi5QTu7gAdJZt-SC-U3Y9bg": "Patient Safety",
    "1S75wiOMH1iY-30123r2rcl3Req29Rr_bwRM6xXOUmfU": "Checklists",
}

# ---------------- helpers ----------------
def safe_str(x: object, default: str = "") -> str:
    """Return a clean string; treat None/NaN/'nan' as empty."""
    try:
        if x is None or (hasattr(pd, "isna") and pd.isna(x)):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return default if s.lower() == "nan" else s

def split_tags(s: str):
    return [t.strip() for t in safe_str(s).split(";") if t.strip()]

def hl(text: str, q: str) -> str:
    s = safe_str(text)
    if not q:
        return s
    return re.sub(re.escape(q), lambda m: f"<mark>{m.group(0)}</mark>", s, flags=re.I)

def norm_link(x) -> str:
    """Return clean http(s) URL or '' if invalid/empty."""
    s = safe_str(x)
    if not s:
        return ""
    try:
        p = urlparse(s)
        return s if p.scheme in ("http", "https") and bool(p.netloc) else ""
    except Exception:
        return ""

def _sheet_id(s: str) -> str:
    """Accept full URL or ID; return spreadsheet ID (or original if name)."""
    s = safe_str(s)
    m = re.search(r"/d/([A-Za-z0-9_-]+)", s)
    if m:
        return m.group(1)
    q = up.urlparse(s).query
    if q:
        qs = dict(up.parse_qsl(q))
        if "id" in qs:
            return qs["id"]
    return s

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Make columns consistent, remove blank rows, and coerce types safely."""
    df = df.copy()

    # Ensure expected columns exist
    for col in ["title", "category", "tags", "owner", "notes", "drive_link"]:
        if col not in df.columns:
            df[col] = ""

    # Coerce strings
    df["title"]    = df["title"].apply(safe_str)
    df["category"] = df["category"].apply(safe_str)
    df["owner"]    = df["owner"].apply(safe_str)
    df["notes"]    = df["notes"].apply(safe_str)
    df["tags"]     = df["tags"].apply(safe_str)

    # Normalize tags back to single string for table; card will split for display
    df["tags"] = df["tags"].apply(lambda s: ";".join(split_tags(s)))

    # Drive link normalization
    df["drive_link"] = df["drive_link"].apply(norm_link)

    # Dates
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # Drop rows that are completely non-useful (title, link AND notes empty)
    mask_empty = (df["title"] == "") & (df["drive_link"] == "") & (df["notes"] == "")
    df = df[~mask_empty].reset_index(drop=True)

    return df

# ---------------- data loaders ----------------
@st.cache_data(ttl=60)
def load_df(single: str) -> pd.DataFrame:
    gc = get_client()
    try:
        sh = gc.open(single)             # by name
    except Exception:
        sh = gc.open_by_key(single)      # by ID
    ws = sh.sheet1
    raw = ws.get_all_records()
    df = pd.DataFrame(raw)
    df = normalize_df(df)
    return df

@st.cache_data(ttl=60)
def load_many(ids_or_names) -> pd.DataFrame:
    gc = get_client()
    frames = []
    for ident in ids_or_names:
        key = _sheet_id(ident)
        try:
            sh = gc.open_by_key(key)
        except Exception:
            sh = gc.open(ident)  # allow names too
        ws = sh.sheet1
        raw = ws.get_all_records()
        df = pd.DataFrame(raw)
        df["__source"] = key
        df = normalize_df(df)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------------- UI setup ----------------
st.set_page_config(page_title="Hospital Accreditation Hub", layout="wide")
st.title("Hospital Accreditation Hub")
PUBLIC_URL = "https://hospital-cert-app-enwsxvnq6npmufwy3ysccf.streamlit.app/"
st.link_button("Open this app (public URL)", PUBLIC_URL, type="secondary")
st.caption("Google Sheets + Google Drive + Streamlit")

# ---------------- config from secrets ----------------
cfg = st.secrets.get("app", {})
SHEET_IDS = cfg.get("sheet_ids", [])
SHEET_NAME = cfg.get("sheet_name")

# load data (prefer multiple sheets)
try:
    if SHEET_IDS:
        df = load_many(SHEET_IDS)
    elif SHEET_NAME:
        df = load_df(SHEET_NAME)
    else:
        st.error("Secrets is missing [app].sheet_ids or [app].sheet_name.")
        st.stop()
except Exception as e:
    st.error(f"Failed to load sheet(s): {e}")
    st.stop()

# top actions
loaded_at = datetime.now(timezone.utc)
st.caption(f"Last refresh: {loaded_at.strftime('%Y-%m-%d %H:%M UTC')}")
if st.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()

# required columns
required_cols = ["title", "drive_link"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    st.error(f"Sheet is missing required columns: {', '.join(missing)}")
    st.stop()

# map source IDs to friendly names (if present)
if "__source" in df.columns:
    df["__source_name"] = df["__source"].map(SHEET_NAME_MAP).fillna(df["__source"])

# ---------------- sidebar filters ----------------
with st.sidebar:
    st.subheader("Search / Filter")
    q = st.text_input("Keyword (title/tags/notes)", "")

    cats = sorted([c for c in df.get("category", []).unique() if isinstance(c, str)]) if "category" in df.columns else []
    sel_cat = st.multiselect("Category", cats, default=[]) if cats else []

    all_tags = sorted({t for ts in df.get("tags", []).tolist() for t in split_tags(ts)}) if "tags" in df.columns else []
    sel_tag = st.multiselect("Tags", all_tags, default=[]) if all_tags else []

    if "__source_name" in df.columns:
        sources = sorted(df["__source_name"].dropna().unique().tolist())
        sel_src = st.multiselect("Source sheet", sources, default=[])
    else:
        sel_src = []

    sort_by = st.selectbox("Sort", ["updated_at desc", "title asc"]) if len(df) > 0 else "title asc"

# ---------------- apply filters ----------------
view = df.copy()

if q:
    ql = q.lower()
    def hit(row):
        fields = [
            safe_str(row.get("title","")),
            safe_str(row.get("tags","")),
            safe_str(row.get("notes","")),
            safe_str(row.get("owner","")),
            safe_str(row.get("category","")),
        ]
        return any(ql in f.lower() for f in fields)
    view = view[view.apply(hit, axis=1)]

if sel_cat and "category" in view.columns:
    view = view[view["category"].isin(sel_cat)]

if sel_tag and "tags" in view.columns:
    view = view[view["tags"].apply(lambda s: any(t in split_tags(s) for t in sel_tag))]

if sel_src and "__source_name" in view.columns:
    view = view[view["__source_name"].isin(sel_src)]

# sort
if len(view) > 0:
    if sort_by == "updated_at desc" and "updated_at" in view.columns:
        view = view.sort_values("updated_at", ascending=False)
    else:
        view = view.sort_values("title", ascending=True, na_position="last")

# export
st.download_button(
    "Download CSV",
    data=view.to_csv(index=False).encode("utf-8-sig"),
    file_name="hospital_hub.csv",
    mime="text/csv",
    use_container_width=True
)

# ---------------- debug (optional) ----------------
with st.expander("Debug: sources loaded", expanded=False):
    st.write("SHEET_IDS from secrets:", SHEET_IDS)
    st.write("Total rows after merge:", len(df))
    if "__source" in df.columns and len(df) > 0:
        counts = df["__source"].value_counts(dropna=False)
        st.dataframe(
            counts.rename_axis("source_id").reset_index(name="rows"),
            use_container_width=True,
        )
        if "__source_name" in df.columns:
            counts2 = df["__source_name"].value_counts(dropna=False)
            st.dataframe(
                counts2.rename_axis("source_name").reset_index(name="rows"),
                use_container_width=True,
            )

# ---------------- results ----------------
st.subheader(f"Items ({len(view)})")
if len(view) == 0:
    st.warning("No items. Adjust filters.")
else:
    for _, row in view.iterrows():
        with st.container(border=True):
            raw_title = row.get("title", "")
            title = hl(raw_title, q)
            cat = safe_str(row.get("category", "-")) or "-"
            owner = safe_str(row.get("owner", "-")) or "-"
            tags_str = " / ".join(split_tags(row.get("tags",""))) or "-"
            upd = row.get("updated_at")
            upd_str = upd.strftime("%Y-%m-%d") if isinstance(upd, pd.Timestamp) else "-"

            st.markdown(f"### {title}", unsafe_allow_html=True)
            meta = f"Category: {cat} | Owner: {owner} | Tags: {tags_str} | Updated: {upd_str}"
            if "__source_name" in row:
                src_name = safe_str(row.get("__source_name", ""))
                if src_name:
                    meta += f" | Source: {src_name}"
            st.write(meta)

            note = safe_str(row.get("notes",""))
            if note:
                st.markdown("> " + hl(note, q), unsafe_allow_html=True)

            link = norm_link(row.get("drive_link"))
            if link:
                st.link_button("Open in Drive", link, use_container_width=True)

    st.divider()
    st.subheader("Table")
    st.dataframe(view, use_container_width=True)
