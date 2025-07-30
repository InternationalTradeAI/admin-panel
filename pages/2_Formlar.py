import streamlit as st
import psycopg2
import pandas as pd

st.title("Formlar")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

def get_forms():
    conn = psycopg2.connect(
        host="database-1.cdm486o0wi80.us-west-1.rds.amazonaws.com",
        port="5432",
        database="itai-demo",
        user="readonly_user",
        password="itaipass"
    )
    query = "SELECT id, created_at FROM forms ORDER BY created_at DESC;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.dataframe(get_forms())
