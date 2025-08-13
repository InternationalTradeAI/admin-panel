#!/usr/bin/env python3
import os
import sys
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection(host, port, dbname, user, password): return psycopg2.connect( host="database-1.cdm486o0wi80.us-west-1.rds.amazonaws.com", port="5432", dbname="itai-demo", user="readonly_user", password="itaipass", )

def fetch_matrix(conn, min_form_id=None, max_form_id=None):
    params = []
    id_filter_sql = ""
    if min_form_id is not None:
        id_filter_sql += " AND COALESCE(sr.form_id, wc_agg.form_id) >= %s"
        params.append(min_form_id)
    if max_form_id is not None:
        id_filter_sql += " AND COALESCE(sr.form_id, wc_agg.form_id) <= %s"
        params.append(max_form_id)

    sql = f"""
    WITH sr AS (
        SELECT form_id, COUNT(*) AS total_search_count
        FROM search_results
        GROUP BY form_id
    ),
    wc_agg AS (
        SELECT
            sr.form_id,
            SUM(CASE WHEN wc.status IN ('EXTRACTED','FAILED') THEN 1 ELSE 0 END) AS processed_done,
            SUM(CASE WHEN wc.status = 'IN_PROGRESS' THEN 1 ELSE 0 END)          AS in_progress_count,
            SUM(CASE WHEN wc.status = 'PENDING'     THEN 1 ELSE 0 END)          AS pending_count
        FROM website_content wc
        JOIN search_results sr ON wc.search_result_id = sr.result_id
        GROUP BY sr.form_id
    )
    SELECT
        COALESCE(sr.form_id, wc_agg.form_id) AS form_id,
        COALESCE(sr.total_search_count, 0)    AS total_search_count,
        COALESCE(wc_agg.processed_done, 0)    AS processed_done,
        COALESCE(wc_agg.in_progress_count, 0) AS in_progress_count,
        COALESCE(wc_agg.pending_count, 0)     AS pending_count
    FROM sr
    FULL OUTER JOIN wc_agg ON sr.form_id = wc_agg.form_id
    WHERE 1=1 {id_filter_sql}
    ORDER BY 1;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def main():
    parser = argparse.ArgumentParser(description="List forms marked in-progress in DB and tell which are actually DONE.")
    parser.add_argument("--host", default=os.getenv("DB_HOST", "database-1.cdm486o0wi80.us-west-1.rds.amazonaws.com"))
    parser.add_argument("--port", default=os.getenv("DB_PORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("DB_NAME", "itai-demo"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "readonly_user"))
    parser.add_argument("--password", default=os.getenv("DB_PASS", "itaipass"))
    parser.add_argument("--min_form_id", type=int)
    parser.add_argument("--max_form_id", type=int)
    args = parser.parse_args()

    conn = get_connection(args.host, args.port, args.dbname, args.user, args.password)
    try:
        rows = fetch_matrix(conn, args.min_form_id, args.max_form_id)
    finally:
        conn.close()

    stuck_ids = []
    still_inprogress = []

    for r in rows:
        fid      = r.get("form_id")
        total    = r.get("total_search_count") or 0
        done_cnt = r.get("processed_done") or 0
        in_prog  = r.get("in_progress_count") or 0
        pending  = r.get("pending_count") or 0

        if fid is None:
            continue  # safety

        db_in_progress = (in_prog > 0) or (pending > 0)
        if not db_in_progress:
            continue

        actually_done = (total > 0) and (done_cnt >= total)

        if actually_done:
            stuck_ids.append(fid)
        else:
            still_inprogress.append(fid)

    print("STILL IN-PROGRESS:")
    print(", ".join(map(str, sorted(still_inprogress))) if still_inprogress else "(none)")

if __name__ == "__main__":
    main()
