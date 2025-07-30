import streamlit as st

st.set_page_config(page_title="Admin Panel", layout="wide")

VALID_PASSWORD = "malii4720"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def login():
    st.title("Giriş")
    password = st.text_input("Şifre", type="password")
    if password == VALID_PASSWORD:
        st.session_state.authenticated = True
        st.rerun()  # yeni sürüm için doğru olan
    elif password != "":
        st.warning("Geçersiz şifre")

if not st.session_state.authenticated:
    login()
else:
    st.success("Giriş başarılı. Sol menüden bir sayfa seçebilirsiniz.")
