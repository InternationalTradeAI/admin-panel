import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import execute_batch
from db_config import get_connection

st.title("Form Durumları ve Trade Score Özetleri")

if "authenticated" not in st.session_state or not st.session_state.authenticated:
    st.warning("Bu sayfayı görmek için önce giriş yapmalısınız.")
    st.stop()

@st.cache_data
def load_data():
    conn = get_connection()
    df_content = pd.read_sql("SELECT form_id, status FROM website_content;", conn)
    df_scores  = pd.read_sql("SELECT form_id, trade_score_int FROM evaluation_reports;", conn)
    conn.close()
    return df_content, df_scores

# --- DB tablo oluşturma ve kaydetme yardımcıları ---
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS form_status_summary (
  form_id            BIGINT PRIMARY KEY,
  total_count        BIGINT,
  extracted_count    BIGINT,
  failed_count       BIGINT,
  failed_percent     NUMERIC(6,2),
  status             TEXT,
  avg_trade_score    NUMERIC(10,2),
  top5_trade_scores  TEXT,
  count_50_plus      BIGINT,
  percent_50_plus    NUMERIC(6,2),
  count_70_plus      BIGINT,
  percent_70_plus    NUMERIC(6,2),
  computed_at        TIMESTAMPTZ DEFAULT now()
);
"""

UPSERT_SQL = """
INSERT INTO form_status_summary
(form_id, total_count, extracted_count, failed_count, failed_percent, status,
 avg_trade_score, top5_trade_scores, count_50_plus, percent_50_plus,
 count_70_plus, percent_70_plus, computed_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
ON CONFLICT (form_id) DO UPDATE SET
  total_count       = EXCLUDED.total_count,
  extracted_count   = EXCLUDED.extracted_count,
  failed_count      = EXCLUDED.failed_count,
  failed_percent    = EXCLUDED.failed_percent,
  status            = EXCLUDED.status,
  avg_trade_score   = EXCLUDED.avg_trade_score,
  top5_trade_scores = EXCLUDED.top5_trade_scores,
  count_50_plus     = EXCLUDED.count_50_plus,
  percent_50_plus   = EXCLUDED.percent_50_plus,
  count_70_plus     = EXCLUDED.count_70_plus,
  percent_70_plus   = EXCLUDED.percent_70_plus,
  computed_at       = now();
"""

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

def save_summary_to_db(display_df: pd.DataFrame):
    rows = []
    for r in display_df.itertuples(index=False):
        def pct_to_float(x):
            if x in (None, 'N/A'): return None
            s = str(x).strip().replace('%','')
            return float(s) if s!='' else None

        failed_percent  = pct_to_float(r.failed_yuzdesi)
        p50             = pct_to_float(r.percent_50_plus)
        p70             = pct_to_float(r.percent_70_plus)
        avg_trade_score = None if r.ortalama_trade_score in (None, 'N/A') else float(r.ortalama_trade_score)
        top5_text       = None if r.top5_trade_score in (None, 'N/A') else str(r.top5_trade_score)

        rows.append((
            int(r.form_id),
            int(r.total_count) if pd.notna(r.total_count) else 0,
            int(getattr(r, "EXTRACTED", 0) or 0),
            int(getattr(r, "FAILED", 0) or 0),
            failed_percent,
            r.status,
            avg_trade_score,
            top5_text,
            None if r.count_50_plus in (None, 'N/A') else int(r.count_50_plus),
            p50,
            None if r.count_70_plus in (None, 'N/A') else int(r.count_70_plus),
            p70
        ))

    conn = get_connection()
    try:
        ensure_table(conn)
        with conn.cursor() as cur:
            execute_batch(cur, UPSERT_SQL, rows, page_size=500)
        conn.commit()
        st.success(f"{len(rows)} satır form_status_summary tablosuna yazıldı / güncellendi.")
    except Exception as e:
        st.error(f"DB yazma hatası: {e}\n(Not: readonly kullanıcıyla bu işlem yapılamaz.)")
        conn.rollback()
    finally:
        conn.close()

# --------- Veri hazırlama ----------
df_content, df_scores = load_data()
df_scores = df_scores.dropna(subset=["trade_score_int"])

grouped = (
    df_content
    .groupby(["form_id", "status"])
    .size()
    .unstack(fill_value=0)
)
grouped["total_count"] = grouped.sum(axis=1)
grouped["failed_yuzdesi"] = (
    grouped.get("FAILED", 0) / grouped["total_count"] * 100
).round(2).astype(str) + "%"

def determine_status(row):
    if row.get("PENDING", 0) > 0:
        return "pending"
    return "done"

grouped["status"] = grouped.apply(determine_status, axis=1)

grouped_indices = grouped.index
avg_scores = df_scores.groupby("form_id")["trade_score_int"].mean().round(2)
top5_scores = df_scores.groupby("form_id")["trade_score_int"].apply(lambda x: sorted(x, reverse=True)[:5])

summary = pd.DataFrame(index=grouped_indices)
summary["ortalama_trade_score"] = summary.index.map(lambda fid: avg_scores.get(fid, None))

def format_top5(fid):
    scores = top5_scores.get(fid, [])
    if not scores:
        return None
    return ", ".join(str(int(v)) for v in scores)
summary["top5_trade_score"] = summary.index.map(format_top5)

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

final_df = pd.concat([grouped, summary, threshold_df], axis=1).reset_index()

cols = [
    "form_id", "total_count", "EXTRACTED", "FAILED", "failed_yuzdesi",
    "status", "ortalama_trade_score", "top5_trade_score",
    "count_50_plus", "percent_50_plus", "count_70_plus", "percent_70_plus"
]
display_df = final_df[cols].sort_values("form_id", ascending=False)

for col in ["ortalama_trade_score", "count_50_plus", "count_70_plus", "top5_trade_score"]:
    display_df[col] = display_df[col].apply(lambda x: 'N/A' if pd.isna(x) else x)
for col in ["percent_50_plus", "percent_70_plus"]:
    display_df[col] = display_df[col].fillna('N/A')

# ---- UI: tablo + butonlar ----
st.dataframe(display_df, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    st.download_button(
        label="CSV Olarak İndir",
        data=display_df.to_csv(index=False).encode("utf-8"),
        file_name="form_ozetleri.csv",
        mime="text/csv"
    )
with c2:
    if st.button("Özetleri DB'ye Kaydet"):
        save_summary_to_db(display_df)
