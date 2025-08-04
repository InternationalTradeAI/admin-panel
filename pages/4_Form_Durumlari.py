import streamlit as st
import pandas as pd
import psycopg2
from db_config import get_connection

st.title("Karşılaştırmalı Form Durumları")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

@st.cache_data
def load_form_status_comparison():
    conn = get_connection()

    forms_df = pd.read_sql("SELECT id, created_at FROM forms;", conn)
    profiles_df = pd.read_sql("SELECT form_id, content_id FROM profiles;", conn)
    reports_df = pd.read_sql("SELECT content_id FROM evaluation_reports;", conn)
    results_df = pd.read_sql("SELECT form_id, result_id FROM search_results;", conn)
    content_df = pd.read_sql("SELECT search_result_id, status FROM website_content;", conn)

    conn.close()

    # Geliştirilmiş Algoritma 1: profile + report bazlı
    forms_df["algo_1_status"] = "not_started"
    prof_group = profiles_df.groupby("form_id")["content_id"].apply(list).reset_index()
    evaluated_set = set(reports_df["content_id"])

    for _, row in prof_group.iterrows():
        fid = row["form_id"]
        cids = row["content_id"]
        if any(cid in evaluated_set for cid in cids):
            forms_df.loc[forms_df["id"] == fid, "algo_1_status"] = "done"
        else:
            forms_df.loc[forms_df["id"] == fid, "algo_1_status"] = "in_progress"

    # Geliştirilmiş Algoritma 2: search_result ve content status bazlı
    extract_fail_status = ["EXTRACTED", "FAILED"]
    result_counts = results_df.groupby("form_id").size().rename("total_search_count")
    content_counts = (
        content_df[content_df["status"].isin(extract_fail_status)]
        .merge(results_df, left_on="search_result_id", right_on="result_id", how="left")
        .groupby("form_id")
        .size()
        .rename("processed_count")
    )

    merged_status = pd.concat([result_counts, content_counts], axis=1).fillna(0).astype(int)
    forms_df = forms_df.merge(merged_status, how="left", left_on="id", right_index=True).fillna(0).astype({"total_search_count": int, "processed_count": int})

    def determine_algo2(row):
        if row["total_search_count"] == 0:
            return "not_started"
        elif row["processed_count"] >= row["total_search_count"]:
            return "done"
        elif row["processed_count"] > 0:
            return "in_progress"
        else:
            return "not_started"

    forms_df["algo_2_status"] = forms_df.apply(determine_algo2, axis=1)

    # Çelişki var mı?
    forms_df["disagreement"] = forms_df["algo_1_status"] != forms_df["algo_2_status"]

    return forms_df[["id", "created_at", "algo_1_status", "algo_2_status", "disagreement", "total_search_count", "processed_count"]].sort_values(by="created_at", ascending=False)

df = load_form_status_comparison()
st.dataframe(df, use_container_width=True)

st.download_button(
    label="Excel Olarak İndir",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="karsilastirma_durumlari.csv",
    mime="text/csv"
)
