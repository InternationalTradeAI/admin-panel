import streamlit as st
import pandas as pd
import psycopg2
from db_config import get_connection

# Sayfa başlığı
st.title("Form Durumları")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

# Veritabanından formların durumlarını alan fonksiyon
@st.cache_data
def get_form_statuses():
    conn = get_connection()

    forms_df = pd.read_sql("SELECT id, created_at FROM forms;", conn)
    profiles_df = pd.read_sql("SELECT form_id, content_id FROM profiles;", conn)
    reports_df = pd.read_sql("SELECT content_id FROM evaluation_reports;", conn)

    conn.close()

    # Başlangıçta tüm formlar 'not_started' kabul edilir
    forms_df["status"] = "not_started"

    # profiles tablosundaki form_id'ler → in_progress
    profile_ids = profiles_df["form_id"].unique()
    forms_df.loc[forms_df["id"].isin(profile_ids), "status"] = "in_progress"

    # profiles + reports → done
    merged = profiles_df.merge(reports_df, on="content_id", how="inner")
    done_ids = merged["form_id"].unique()
    forms_df.loc[forms_df["id"].isin(done_ids), "status"] = "done"

    return forms_df.sort_values(by="created_at", ascending=False)

# Veriyi al
form_status_df = get_form_statuses()

# Filtreleme: Duruma göre
status_filter = st.multiselect("Duruma göre filtrele", ["done", "in_progress", "not_started"], default=["done", "in_progress", "not_started"])
filtered_df = form_status_df[form_status_df["status"].isin(status_filter)]

# Göster
st.dataframe(filtered_df, use_container_width=True)

# Excel olarak indir
st.download_button(
    label="Excel Olarak İndir",
    data=filtered_df.to_csv(index=False).encode("utf-8"),
    file_name="form_durumlari.csv",
    mime="text/csv"
)
