import streamlit as st
import psycopg2
import json
from datetime import datetime
from db_config import get_connection

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

def fetch_form_data(form_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT form_data FROM forms WHERE id = %s", (form_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

def insert_new_form(form_data):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO forms (form_data, created_at, updated_at) VALUES (%s, %s, %s)",
        (json.dumps(form_data), datetime.utcnow(), datetime.utcnow())
    )
    conn.commit()
    cur.close()
    conn.close()

# Arayüz
st.title("Form Oluştur")

form_id = st.text_input("Form ID'yi girin")

if form_id:
    try:
        data = fetch_form_data(int(form_id))
        if data:
            st.success("Form başarıyla yüklendi.")
            pretty_json = json.dumps(data, indent=4, ensure_ascii=False)
            edited = st.text_area("Form düzenleme", value=pretty_json, height=400)

            if st.button("Yeni Form Olarak Kaydet"):
                try:
                    new_form_data = json.loads(edited)
                    insert_new_form(new_form_data)
                    st.success("Yeni form başarıyla kaydedildi.")
                except Exception as e:
                    st.error(f"JSON hatası veya kayıt hatası: {e}")
        else:
            st.warning("Bu ID ile form bulunamadı.")
    except Exception as e:
        st.error(f"Geçersiz ID veya sorgu hatası: {e}")
