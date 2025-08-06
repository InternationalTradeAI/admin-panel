import streamlit as st
import pandas as pd
import psycopg2
from db_config import get_connection

st.title("Form Durumları")

# Giriş kontrolü
if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

@st.cache_data
def get_algo2_form_statuses():
    conn = get_connection()

    # Gerekli tabloları al
    forms_df = pd.read_sql("SELECT id, created_at FROM forms;", conn)
    results_df = pd.read_sql("SELECT form_id, result_id FROM search_results;", conn)
    content_df = pd.read_sql("SELECT search_result_id, status FROM website_content;", conn)

    conn.close()

    # Toplam search count (her form_id için kaç tane result var)
    total_search = results_df.groupby("form_id").size().rename("total_search_count")

    # EXTRACTED veya FAILED olarak işaretlenen içerikler
    extract_fail_status = ["EXTRACTED", "FAILED"]
    processed = (
        content_df[content_df["status"].isin(extract_fail_status)]
        .merge(results_df, left_on="search_result_id", right_on="result_id", how="left")
        .groupby("form_id")
        .size()
        .rename("processed_count")
    )

    # Sonuçları birleştir
    merged = pd.concat([total_search, processed], axis=1).fillna(0).astype(int)
    forms_df = forms_df.merge(merged, how="left", left_on="id", right_index=True).fillna(0).astype({"total_search_count": int, "processed_count": int})

    # Durum hesaplama
    def determine_status(row):
        if row["total_search_count"] == 0:
            return "not_started"
        elif row["processed_count"] >= row["total_search_count"]:
            return "done"
        elif row["processed_count"] > 0:
            return "in_progress"
        else:
            return "not_started"

    forms_df["status"] = forms_df.apply(determine_status, axis=1)

    return forms_df[["id", "created_at", "status", "total_search_count", "processed_count"]].sort_values(by="created_at", ascending=False)

# Veriyi al ve göster
df = get_algo2_form_statuses()
st.dataframe(df, use_container_width=True)

# İndir
st.download_button(
    label="Excel Olarak İndir",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="form_durumlari_algo2.csv",
    mime="text/csv"
)
