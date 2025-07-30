import psycopg2

def get_connection():
    return psycopg2.connect(
        host="database-1.cdm486o0wi80.us-west-1.rds.amazonaws.com",
        port=5432,
        dbname="itai-demo",
        user="readonly_user",
        password="itaipass"
    )
