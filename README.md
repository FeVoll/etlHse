# ETL Репликация из MongoDB в PostgreSQL с использованием Apache Airflow

## Архитектура
### MongoDB (Источник данных)
Используются следующие коллекции:
- `user_sessions`
- `product_price_history`
- `event_logs`
- `support_tickets`
- `user_recommendations`
- `moderation_queue`
- `search_queries`

### PostgreSQL (Целевая база данных)
Для каждой коллекции создается своя таблица с дополнительными полями:
- `user_sessions`: вычисляется поле `session_duration` (продолжительность сессии в минутах).
- `product_price_history`: вычисляется поле `price_trend` (значения "increasing", "decreasing" или "stable").
- `support_tickets`: создается поле `all_messages`, в которое объединяются отдельные сообщения.

### Docker Compose (Развертывание)
Среда включает контейнеры для MongoDB, PostgreSQL и Apache Airflow.

### Создание необходимых директорий (на примере Windows). Укажите в docker-compose нужную директорию
```
C:/Users/UserPC/Documents/etl3/airflow/dags
C:/Users/UserPC/Documents/etl3/airflow/logs
C:/Users/UserPC/Documents/etl3/airflow/plugins
```

### Dockerfile
`Dockerfile` (рядом с `docker-compose.yml`) со следующим содержимым ставит на вируталку pymongo:
```dockerfile
FROM apache/airflow:2.5.1
USER airflow
RUN pip install --no-cache-dir pymongo
```

### docker-compose.yml
Используйте следующий файл для развертывания контейнеров:
```yaml
version: '3'
services:
  mongodb:
    image: mongo:latest
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongo-data:/data/db

  postgres:
    image: postgres:13
    container_name: postgres
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: etl_db
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  airflow-webserver:
    build: .
    container_name: airflow-webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__FERNET_KEY=(вставьте ключ)
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@postgres:5432/etl_db
    volumes:
      - C:/Users/UserPC/Documents/etl3/airflow/dags:/opt/airflow/dags
      - C:/Users/UserPC/Documents/etl3/airflow/logs:/opt/airflow/logs
      - C:/Users/UserPC/Documents/etl3/airflow/plugins:/opt/airflow/plugins
    depends_on:
      - postgres
      - mongodb
    ports:
      - "8080:8080"
    command: webserver

  airflow-scheduler:
    build: .
    container_name: airflow-scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__FERNET_KEY=(вставьте ключ)
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@postgres:5432/etl_db
    volumes:
      - C:/Users/UserPC/Documents/etl3/airflow/dags:/opt/airflow/dags
      - C:/Users/UserPC/Documents/etl3/airflow/logs:/opt/airflow/logs
      - C:/Users/UserPC/Documents/etl3/airflow/plugins:/opt/airflow/plugins
    depends_on:
      - postgres
      - mongodb
      - airflow-webserver
    command: scheduler

volumes:
  mongo-data:
  postgres-data:
```

## Этапы ETL-процесса
### Извлечение данных (Extract)
На этом этапе данные загружаются из MongoDB с использованием PyMongo. Каждый DAG извлекает данные из соответствующей коллекции MongoDB и передаёт их в этап трансформации.

### Трансформация данных (Transform)
На этапе трансформации выполняются следующие преобразования:
- Преобразование объектов `datetime` в строки формата ISO.
- Для `user_sessions`: вычисляется продолжительность сессии (`session_duration`) в минутах.
- Для `product_price_history`: список изменений цены сортируется по дате, после чего определяется тренд изменения цены (`price_trend`) — значения "increasing", "decreasing" или "stable".
- Для `support_tickets`: отдельные сообщения объединяются в одно поле (`all_messages`).

### Загрузка данных (Load)
Трансформированные данные загружаются в PostgreSQL с помощью SQLAlchemy. Каждая коллекция MongoDB соответствует отдельной таблице в PostgreSQL, и данные вставляются с обработкой дубликатов (например, с использованием `ON CONFLICT DO NOTHING`).

## Аналитические витрины
### Витрина активности пользователей
Аналитическая витрина активности пользователей формируется на основе данных из таблицы `user_sessions`. Для каждого пользователя (`user_id`) вычисляется:
- Общее количество сессий (`session_count`).
- Средняя продолжительность сессии (`avg_session_duration`).

### Витрина эффективности работы поддержки
Аналитическая витрина эффективности работы поддержки создается на основе данных из таблицы `support_tickets`. Для каждого типа проблемы (`issue_type`) рассчитывается:
- Общее количество тикетов (`ticket_count`).
- Среднее время решения проблемы (`avg_resolution_time_minutes`).

Среднее время вычисляется как разница между временем обновления тикета (`updated_at`) и временем его создания (`created_at`), переведенная в минуты.

## Проверка данных после выполнения
После запуска выполните следующие SQL-запросы для проверки данных:
```sql
SELECT * FROM user_activity_vitrine;
SELECT * FROM support_efficiency_vitrine;
```
