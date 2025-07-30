import streamlit as st
import psycopg2
import pandas as pd
from db_config import get_connection

st.title("Form Sonuçları")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

def run_query(form_id_filter=None):
    conn = get_connection()

    base_query = """
    SELECT
      sr.url                    AS source_url,
      q.country                 AS search_country,
      p.form_id,
      er.trade_score_int        AS trade_score,
      er.country_score_int      AS country_score,
      f.form_data  ->> 'targetMarket' AS given_country,
      q.query,
      f.form_data  ->> 'keywords'     AS keywords,
      p.raw_profile             AS profile_json,
      er.raw_report             AS evaluation_json
    FROM website_content wc
    JOIN search_results     sr  ON wc.search_result_id = sr.result_id
    JOIN queries            q   ON sr.query_id          = q.query_id
    JOIN profiles           p   ON wc.content_id        = p.content_id
    JOIN forms              f   ON p.form_id            = f.id
    JOIN evaluation_reports er  ON wc.content_id        = er.content_id
    WHERE er.trade_score_int    > 50
      AND er.country_score_int > 0
    """

    if form_id_filter:
        base_query += f" AND p.form_id = {int(form_id_filter)} "

    base_query += " ORDER BY wc.content_id"

    df = pd.read_sql(base_query, conn)
    conn.close()
    return df

form_id = st.text_input("Form ID ile filtrele (opsiyonel):")

if st.button("Verileri Getir"):
    try:
        df = run_query(form_id if form_id else None)
        if df.empty:
            st.info("Hiç sonuç bulunamadı.")
        else:
            st.dataframe(df)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("Excel olarak indir", csv, "form_sonuclari.csv", "text/csv")
    except Exception as e:
        st.error(f"Hata oluştu: {e}")
