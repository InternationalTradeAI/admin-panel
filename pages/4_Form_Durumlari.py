import streamlit as st
import pandas as pd
import psycopg2
from db_config import get_connection

st.title("Form Durumları")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

@st.cache_data
def get_form_statuses():
    conn = get_connection()

    query = """
    WITH content_counts AS (
        SELECT form_id, COUNT(*) AS total_contents
        FROM profiles
        GROUP BY form_id
    ),
    evaluated_counts AS (
        SELECT p.form_id, COUNT(*) AS evaluated_contents
        FROM profiles p
        JOIN evaluation_reports e ON p.content_id = e.content_id
        GROUP BY p.form_id
    ),
    statuses AS (
        SELECT
            f.id AS form_id,
            f.created_at,
            COALESCE(cc.total_contents, 0) AS total,
            COALESCE(ec.evaluated_contents, 0) AS evaluated,
            CASE
                WHEN cc.total_contents IS NULL THEN 'not_started'
                WHEN COALESCE(ec.evaluated_contents, 0) = cc.total_contents THEN 'done'
                WHEN COALESCE(ec.evaluated_contents, 0) > 0 THEN 'in_progress'
                ELSE 'in_progress'
            END AS status
        FROM forms f
        LEFT JOIN content_counts cc ON f.id = cc.form_id
        LEFT JOIN evaluated_counts ec ON f.id = ec.form_id
    )
    SELECT * FROM statuses
    ORDER BY created_at DESC;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Veri çek
form_status_df = get_form_statuses()

# Durum filtresi
status_filter = st.multiselect(
    "Duruma göre filtrele",
    ["done", "in_progress", "not_started"],
    default=["done", "in_progress", "not_started"]
)
filtered_df = form_status_df[form_status_df["status"].isin(status_filter)]

# Gösterim
st.dataframe(filtered_df, use_container_width=True)

# Excel indirme
st.download_button(
    label="Excel Olarak İndir",
    data=filtered_df.to_csv(index=False).encode("utf-8"),
    file_name="form_durumlari.csv",
    mime="text/csv"
)
