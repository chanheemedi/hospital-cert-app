# -*- coding: utf-8 -*-
import streamlit as st, pandas as pd, gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

@st.cache_resource
def get_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_data(ttl=60)
def load_df(sheet_name: str) -> pd.DataFrame:
    gc = get_client()
    try:
        sh = gc.open(sheet_name)        # open by name
    except Exception:
        sh = gc.open_by_key(sheet_name) # fallback: open by ID
    ws = sh.sheet1
    df = pd.DataFrame(ws.get_all_records())
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    if "tags" in df.columns:
        df["tags"] = df["tags"].fillna("").astype(str)
    return df

def split_tags(s: str):
    return [t.strip() for t in str(s).split(";") if t.strip()]

st.set_page_config(page_title="Hospital Accreditation Hub", layout="wide")
st.title("Hospital Accreditation Hub")
PUBLIC_URL = "https://hospital-cert-app-enwsxvnq6npmufwy3ysccf.streamlit.app/"
st.link_button("Open this app (public URL)", PUBLIC_URL, type="secondary")

st.caption("Google Sheets + Google Drive + Streamlit")

cfg = st.secrets.get("app", {})
SHEET_NAME = cfg.get("sheet_name")
if not SHEET_NAME:
    st.error("Missing [app].sheet_name in Secrets.")
    st.stop()

try:
    df = load_df(SHEET_NAME)
except Exception as e:
    st.error(f"Failed to load sheet: {e}")
    st.stop()

with st.sidebar:
    st.subheader("Search / Filter")
    q = st.text_input("Keyword (title/tags/notes)", "")
    cats = sorted([c for c in df.get("category", []).unique() if isinstance(c, str)]) if "category" in df.columns else []
    sel_cat = st.multiselect("Category", cats, default=[])
    all_tags = sorted({t for ts in df.get("tags", []).tolist() for t in split_tags(ts)}) if "tags" in df.columns else []
    sel_tag = st.multiselect("Tags", all_tags, default=[])
    sort_by = st.selectbox("Sort", ["updated_at desc", "title asc"]) if len(df) > 0 else "title asc"

view = df.copy()
if q:
    ql = q.lower()
    def hit(row):
        fields = [
            str(row.get("title","")),
            str(row.get("tags","")),
            str(row.get("notes","")),
            str(row.get("owner","")),
            str(row.get("category","")),
        ]
        return any(ql in f.lower() for f in fields)
    view = view[view.apply(hit, axis=1)]
if sel_cat and "category" in view.columns:
    view = view[view["category"].isin(sel_cat)]
if sel_tag and "tags" in view.columns:
    view = view[view["tags"].apply(lambda s: any(t in split_tags(s) for t in sel_tag))]

if len(view) > 0:
    if sort_by == "updated_at desc" and "updated_at" in view.columns:
        view = view.sort_values("updated_at", ascending=False)
    else:
        view = view.sort_values("title", ascending=True, na_position="last")

st.subheader(f"Items ({len(view)})")
if len(view) == 0:
    st.warning("No items. Adjust filters.")
else:
    for _, row in view.iterrows():
        with st.container(border=True):
            title = row.get("title", "(no title)")
            cat = row.get("category", "-")
            owner = row.get("owner", "-")
            tags = " / ".join(split_tags(row.get("tags",""))) or "-"
            upd = row.get("updated_at")
            upd_str = upd.strftime("%Y-%m-%d") if isinstance(upd, pd.Timestamp) else "-"
            st.markdown(f"### {title}")
            st.write(f"Category: {cat} | Owner: {owner} | Tags: {tags} | Updated: {upd_str}")
            note = row.get("notes","")
            if note:
                st.markdown(f"> {note}")
            link = row.get("drive_link","")
            if link:
                st.link_button("Open in Drive", link, use_container_width=True)

    st.divider()
    st.subheader("Table")
    st.dataframe(view, use_container_width=True)
