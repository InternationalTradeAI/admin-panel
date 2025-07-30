import streamlit as st
import psycopg2
import pandas as pd
from db_config import get_connection

st.title("Formlar")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

def get_forms():
    conn = get_connection()
    query = "SELECT id, created_at FROM forms ORDER BY created_at DESC;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.dataframe(get_forms())
