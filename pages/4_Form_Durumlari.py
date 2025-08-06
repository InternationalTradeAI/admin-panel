import pandas as pd
import streamlit as st
import psycopg2
from db_config import get_connection

st.title("Form Durumları ve Trade Score Özetleri")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

@st.cache_data
def load_data():
    conn = get_connection()
    df_content = pd.read_sql("SELECT form_id, status FROM website_content;", conn)
    df_scores = pd.read_sql("SELECT form_id, trade_score_int FROM evaluation_reports;", conn)
    conn.close()
    return df_content, df_scores

# Veriyi yükle
df_content, df_scores = load_data()
# Skorları temizle
df_scores = df_scores.dropna(subset=["trade_score_int"])

# Status sayıları
grouped = (
    df_content
    .groupby(["form_id", "status"])  
    .size()
    .unstack(fill_value=0)
)
# Toplam
grouped["total_count"] = grouped.sum(axis=1)
# Failed yüzdesi
grouped["failed_yuzdesi"] = (
    grouped.get("FAILED", 0) / grouped["total_count"] * 100
).round(2).astype(str) + "%"

# Status belirle
def determine_status(row):
    if row.get("PENDING", 0) > 0:
        return "pending"
    return "done"

grouped["status"] = grouped.apply(determine_status, axis=1)

# Trade skor özetleri
grouped_indices = grouped.index
avg_scores = df_scores.groupby("form_id")["trade_score_int"].mean().round(2)
top5_scores = df_scores.groupby("form_id")["trade_score_int"].apply(lambda x: sorted(x, reverse=True)[:5])

summary = pd.DataFrame(index=grouped_indices)
summary["ortalama_trade_score"] = summary.index.map(lambda fid: avg_scores.get(fid, None))
# top5'i string formatta hazırla
def format_top5(fid):
    scores = top5_scores.get(fid, [])
    if not scores:
        return None
    return ", ".join(str(int(v)) for v in scores)
summary["top5_trade_score"] = summary.index.map(format_top5)

# Eşik istatistikleri
def compute_threshold_stats(fid):
    scores = df_scores[df_scores["form_id"] == fid]["trade_score_int"]
    total = len(scores)
    if total == 0:
        return pd.Series({
            "count_50_plus": None,
            "percent_50_plus": None,
            "count_70_plus": None,
            "percent_70_plus": None
        })
    count_50 = (scores > 50).sum()
    percent_50 = f"{count_50 / total * 100:.2f}%"
    count_70 = (scores > 70).sum()
    percent_70 = f"{count_70 / total * 100:.2f}%"
    return pd.Series({
        "count_50_plus": count_50 if count_50 > 0 else None,
        "percent_50_plus": percent_50 if count_50 > 0 else None,
        "count_70_plus": count_70 if count_70 > 0 else None,
        "percent_70_plus": percent_70 if count_70 > 0 else None
    })

threshold_df = pd.concat(
    [compute_threshold_stats(fid) for fid in grouped_indices],
    axis=1
).T
threshold_df.index = grouped_indices

# Son tablo
final_df = pd.concat([grouped, summary, threshold_df], axis=1).reset_index()

# Görüntülenecek sütunlar
cols = [
    "form_id", "total_count", "EXTRACTED", "FAILED", "failed_yuzdesi",
    "status", "ortalama_trade_score", "top5_trade_score",
    "count_50_plus", "percent_50_plus", "count_70_plus", "percent_70_plus"
]

display_df = final_df[cols].sort_values("form_id", ascending=False)

# None ve değer olmayanları 'N/A' olarak değiştir
for col in ["ortalama_trade_score", "count_50_plus", "count_70_plus", "top5_trade_score"]:
    display_df[col] = display_df[col].apply(lambda x: 'N/A' if pd.isna(x) else x)
for col in ["percent_50_plus", "percent_70_plus"]:
    display_df[col] = display_df[col].fillna('N/A')

# Göster ve indir
st.dataframe(display_df, use_container_width=True)
st.download_button(
    label="CSV Olarak İndir",
    data=display_df.to_csv(index=False).encode("utf-8"),
    file_name="form_ozetleri.csv",
    mime="text/csv"
)
