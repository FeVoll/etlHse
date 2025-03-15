from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2, json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('mongo_to_postgres',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          catchup=False)


def convert_datetimes(obj):
    if isinstance(obj, dict):
        return {k: convert_datetimes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetimes(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj


def extract_data(collection, xcom_key, **kwargs):
    client = MongoClient("mongodb://mongodb:27017/")
    db = client.etl_db
    data = list(db[collection].find())
    for d in data:
        d['_id'] = str(d['_id'])
    data = convert_datetimes(data)
    kwargs['ti'].xcom_push(key=xcom_key, value=data)
    print(f"Extracted {len(data)} docs from {collection}")


def transform_data(collection, extract_task_id, xcom_key, **kwargs):
    from datetime import datetime
    ti = kwargs['ti']
    data = ti.xcom_pull(key=xcom_key, task_ids=extract_task_id)
    if data is None:
        data = []

    # Пример трансформаций:
    if collection == 'user_sessions':
        transformed = []
        for record in data:
            try:
                start = datetime.fromisoformat(record.get("start_time"))
                end = datetime.fromisoformat(record.get("end_time"))
                duration = (end - start).total_seconds() / 60  # продолжительность в минутах
                record["session_duration"] = duration
            except Exception as e:
                record["session_duration"] = None
            transformed.append(record)
        data = transformed
    elif collection == 'product_price_history':
        transformed = []
        for record in data:
            price_changes = record.get("price_changes", [])
            try:
                sorted_changes = sorted(price_changes, key=lambda x: x["date"])
                if sorted_changes:
                    first_price = sorted_changes[0]["price"]
                    last_price = sorted_changes[-1]["price"]
                    if last_price > first_price:
                        record["price_trend"] = "increasing"
                    elif last_price < first_price:
                        record["price_trend"] = "decreasing"
                    else:
                        record["price_trend"] = "stable"
            except Exception as e:
                record["price_trend"] = "unknown"
            transformed.append(record)
        data = transformed
    elif collection == 'support_tickets':
        transformed = []
        for record in data:
            messages = record.get("messages", [])
            record["all_messages"] = " ".join(messages) if messages else ""
            transformed.append(record)
        data = transformed

    ti.xcom_push(key=xcom_key, value=data)
    print(f"Transformed {len(data)} docs for {collection}")


def load_data(collection, transform_task_id, xcom_key, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key=xcom_key, task_ids=transform_task_id)
    if data is None:
        data = []
    conn = psycopg2.connect(host="postgres", database="etl_db", user="user", password="password")
    cur = conn.cursor()

    if collection == 'user_sessions':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR PRIMARY KEY,
                user_id INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                pages_visited TEXT,
                device VARCHAR,
                actions TEXT,
                session_duration NUMERIC
            );
        """)
    elif collection == 'product_price_history':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_price_history (
                product_id VARCHAR PRIMARY KEY,
                price_changes TEXT,
                current_price NUMERIC,
                currency VARCHAR,
                price_trend VARCHAR
            );
        """)
    elif collection == 'event_logs':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_logs (
                event_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                event_type VARCHAR,
                details TEXT
            );
        """)
    elif collection == 'support_tickets':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                ticket_id VARCHAR PRIMARY KEY,
                user_id INTEGER,
                status VARCHAR,
                issue_type VARCHAR,
                messages TEXT,
                all_messages TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
        """)
    elif collection == 'user_recommendations':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_recommendations (
                user_id INTEGER,
                recommended_products TEXT,
                last_updated TIMESTAMP
            );
        """)
    elif collection == 'moderation_queue':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS moderation_queue (
                review_id VARCHAR PRIMARY KEY,
                user_id INTEGER,
                product_id VARCHAR,
                review_text TEXT,
                rating INTEGER,
                moderation_status VARCHAR,
                flags TEXT,
                submitted_at TIMESTAMP
            );
        """)
    elif collection == 'search_queries':
        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_queries (
                query_id VARCHAR PRIMARY KEY,
                user_id INTEGER,
                query_text TEXT,
                timestamp TIMESTAMP,
                filters TEXT,
                results_count INTEGER
            );
        """)
    conn.commit()

    for d in data:
        if collection == 'user_sessions':
            cur.execute("""
                INSERT INTO user_sessions 
                (session_id, user_id, start_time, end_time, pages_visited, device, actions, session_duration) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (session_id) DO NOTHING;
            """, (
                d.get("session_id"),
                d.get("user_id"),
                d.get("start_time"),
                d.get("end_time"),
                json.dumps(d.get("pages_visited")),
                d.get("device"),
                json.dumps(d.get("actions")),
                d.get("session_duration")
            ))
        elif collection == 'product_price_history':
            cur.execute("""
                INSERT INTO product_price_history 
                (product_id, price_changes, current_price, currency, price_trend)
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (product_id) DO NOTHING;
            """, (
                d.get("product_id"),
                json.dumps(d.get("price_changes")),
                d.get("current_price"),
                d.get("currency"),
                d.get("price_trend")
            ))
        elif collection == 'event_logs':
            cur.execute("""
                INSERT INTO event_logs 
                (event_id, timestamp, event_type, details)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """, (
                d.get("event_id"),
                d.get("timestamp"),
                d.get("event_type"),
                d.get("details")
            ))
        elif collection == 'support_tickets':
            cur.execute("""
                INSERT INTO support_tickets 
                (ticket_id, user_id, status, issue_type, messages, all_messages, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticket_id) DO NOTHING;
            """, (
                d.get("ticket_id"),
                d.get("user_id"),
                d.get("status"),
                d.get("issue_type"),
                json.dumps(d.get("messages")),
                d.get("all_messages"),
                d.get("created_at"),
                d.get("updated_at")
            ))
        elif collection == 'user_recommendations':
            cur.execute("""
                INSERT INTO user_recommendations 
                (user_id, recommended_products, last_updated)
                VALUES (%s, %s, %s);
            """, (
                d.get("user_id"),
                json.dumps(d.get("recommended_products")),
                d.get("last_updated")
            ))
        elif collection == 'moderation_queue':
            cur.execute("""
                INSERT INTO moderation_queue 
                (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING;
            """, (
                d.get("review_id"),
                d.get("user_id"),
                d.get("product_id"),
                d.get("review_text"),
                d.get("rating"),
                d.get("moderation_status"),
                json.dumps(d.get("flags")),
                d.get("submitted_at")
            ))
        elif collection == 'search_queries':
            cur.execute("""
                INSERT INTO search_queries 
                (query_id, user_id, query_text, timestamp, filters, results_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (query_id) DO NOTHING;
            """, (
                d.get("query_id"),
                d.get("user_id"),
                d.get("query_text"),
                d.get("timestamp"),
                json.dumps(d.get("filters")),
                d.get("results_count")
            ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(data)} docs into {collection}")


with dag:
    collections = [
        "user_sessions",
        "product_price_history",
        "event_logs",
        "support_tickets",
        "user_recommendations",
        "moderation_queue",
        "search_queries"
    ]

    for coll in collections:
        with TaskGroup(group_id=coll) as tg:
            extract_task_id = f"extract_{coll}"
            transform_task_id = f"transform_{coll}"
            load_task_id = f"load_{coll}"

            extract = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_data,
                op_kwargs={"collection": coll, "xcom_key": coll},
            )

            transform = PythonOperator(
                task_id=transform_task_id,
                python_callable=transform_data,
                op_kwargs={
                    "collection": coll,
                    "extract_task_id": f"{coll}.{extract_task_id}",
                    "xcom_key": coll
                },
            )

            load = PythonOperator(
                task_id=load_task_id,
                python_callable=load_data,
                op_kwargs={
                    "collection": coll,
                    "transform_task_id": f"{coll}.{transform_task_id}",
                    "xcom_key": coll
                },
            )

            extract >> transform >> load
