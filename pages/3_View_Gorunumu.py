import streamlit as st
import psycopg2
import pandas as pd
import json

st.title("View Görünümü")

# Giriş kontrolü
if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

# Veritabanından form verisini çek
def get_prettified_forms():
    conn = psycopg2.connect(
        host="database-1.cdm486o0wi80.us-west-1.rds.amazonaws.com",
        port="5432",
        database="itai-demo",
        user="readonly_user",
        password="itaipass"
    )
    query = "SELECT id, created_at, form_data FROM forms ORDER BY created_at DESC;"
    df = pd.read_sql(query, conn)
    conn.close()

    df["target_market"] = df["form_data"].apply(lambda x: x.get("targetMarket"))
    df["keywords"] = df["form_data"].apply(lambda x: x.get("keywords"))
    df["formatted_form_data"] = df["form_data"].apply(
        lambda x: json.dumps(x, indent=2, ensure_ascii=False)
    )
    return df

df = get_prettified_forms()

# Filtre arayüzü
form_id_filter = st.text_input("Form ID:")
target_options = ["(Hepsi)"] + sorted(df["target_market"].dropna().unique())
target_filter = st.selectbox("Hedef ülke:", target_options)
keyword_filter = st.text_input("Anahtar kelime:")

# Filtreleme işlemi
filtered_df = df.copy()

if form_id_filter:
    try:
        form_id_int = int(form_id_filter)
        filtered_df = filtered_df[filtered_df["id"] == form_id_int]
    except ValueError:
        st.error("Geçerli bir ID girilmelidir.")
else:
    if target_filter != "(Hepsi)":
        filtered_df = filtered_df[filtered_df["target_market"] == target_filter]

    if keyword_filter:
        filtered_df = filtered_df[
            filtered_df["keywords"].str.contains(keyword_filter, case=False, na=False)
        ]

# Sonuçları göster
if filtered_df.empty:
    st.info("Eşleşen kayıt bulunamadı.")
else:
    st.dataframe(filtered_df[["id", "created_at", "target_market", "keywords", "formatted_form_data"]])
