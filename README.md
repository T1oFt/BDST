# BDST
Репозиторий для лабораторных работ по предмету "Технологии хранения больших данных"

источник данных: Huggingface.co

# Лабораторная работа №1 “Работа с Airflow. ETL-процесс.”: 
В рамках данной работы вам необходимо реализовать ETL процесс, отвечающий за сбор и загрузку сырых данных в хранилище (слой ODS). База данных должна быть выбрана командой, выбор необходимо аргументировать. Оркестрация ETL процессов должна быть реализована с помощью Apache Airflow (https://airflow.apache.org/). 
## Этапы выполнения:
- 1 Развернуть сервис Airflow в Docker-контейнере, используя docker-compose 
конфигурацию (примеры можно найти в официальной документации, 
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html); 
- 2 Выбрать 3 различных сервиса для хранения данных. Например: s3, mongodb, 
oracle. Добавить конфигурацию для развертывания хранилищ в docker-compose. 
- 3 Реализовать не менее 3 различных ETL процессов (DAGов). При нехватке 
данных на одной платформе, данные можно брать с нескольких. Например: ozon + wildberries, aviasales + tutu.ru; Данные можно разделять логически в рамках одного источника: комментарии, товары, отзывы. 
- 4 В результате работы ETL процессов данные должны быть выгружены в выбранные базы данных; 
- 5 Провести сравнительный анализ выбранных хранилищ данных. Сравнительные критерии необходимо выбрать самостоятельно. Выбрать наиболее подходящее хранилище для полученных данных. 

Для защиты необходимо предоставить отчет, описывающий этапы выполнения работы, а также исходный код ETL процессов и docker-compose файл. 

Обязательным условием является демонстрация работы: веб интерфейс Airflow, выгруженные данные в базах данных, сравнительный анализ в виде графиков и/или таблиц.

## Отчёт

### Подготовка окружения
Перед началом реализации ETL-процессов была выполнена подготовка окружения.

1. Создание репозитория и рабочей директории
2. Конфигурация Apache Airflow для Docker Compose была загружена из документации:
`https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html"`
3. Создан кастомный dockerfile для установки дополнительных зависимостей.
4. В файл docker-compose.yml были добавлены три сервиса для реализации слоя ODS: `user-postgres`, `mongo`, `clickhouse` с соответствующими volumes: `user-pgdata:`, `mongodata:`, `clickhousedata:`.
5. Была успешно проверена работоспособность окружения.


### Реализация ETL-процессов
После изучения API huggingface были сформированы 3 ETL-процесса:
1. Сбор данных популярных моделей по параметру trending_score. 
2. Сбор данных статей из раздела daily papers.
3. Сбор данных постов из раздела community/posts за 1 день.

для получения данных использовалась библиотека huggingface_hub, а также bs4 для получения id статей.

#### DAG 1. models — сбор данных популярных моделей

- extract - вызов huggingface_hub.list_models(sort='trending_score', limit=50) для получения топ-50 трендовых моделей .

- transform - парсинг тегов в категории (language, library, task, license, base_models, modification, region, diffusers_pipeline, deploy, dataset, arxiv), извлечение owner из id, форматирование datetime, сбор метрик (downloads, likes, trending_score).

- load - одновременная загрузка в PostgreSQL, MongoDB и ClickHouse

    - PostgreSQL: таблица models_ods

    - MongoDB: коллекция models_ods

    - ClickHouse: таблица models_ods

эти данные позволят узнать параметры актуальных моделей и узнать статистику скачиваний и лайков.

#### DAG 2. daily_papers — сбор данных актуальных статей

- extract - парсинг https://huggingface.co/papers/date/{дата} через BeautifulSoup для извлечения arXiv ID (формат XXXX.XXXXX), последующий вызов huggingface_hub.paper_info() для полной информации.

- transform - извлечение ключевых полей (id, authors, title, summary, upvotes, published_at, submitted_by), форматирование datetime.

- load - одновременная загрузка в PostgreSQL, MongoDB и ClickHouse

    - PostgreSQL: таблица papers_ods

    - MongoDB: коллекция papers_ods

    - ClickHouse: таблица papers_ods

эти данные позволят изучить актуальные темы исследований,которые интересны пользователям.


#### DAG 3. posts — сбор данных постов за последний день

- extract - API-запросы https://huggingface.co/api/posts с пагинацией (skip=0,10,20...), фильтрация постов за последние 24 часа по publishedAt .

- transform - извлечение полей (slug, author_name/id, content_raw, published_at/updated_at, total_unique_impressions, num_comments), парсинг datetime в ISO.

- load - одновременная загрузка в PostgreSQL, MongoDB и ClickHouse

    - PostgreSQL: таблица posts_ods

    - MongoDB: коллекция posts_ods

    - ClickHouse: таблица posts_ods

эти данные позволят изучить активность людей на huggingface, а также темы, которые они обсуждают.

### Сравнительный анализ выбранных хранилищ данных

Для сравнения выбранных хранилищ данных был разработан тестовый DAG. Заранее были выгружены 10000 записей моделей. Каждая база данных тестировалась по 3-м параметрам: скорость записи и скорость исполнения запросов, обычных и с использованием join.

#### Результаты тестов

=== WRITE PERFORMANCE (10k records | 1k records | 100 records) ===
   - PostgreSQL : 2.387s | 0.276s | 0.049s
   - MongoDB    : 0.585s | 0.064s | 0.049s
   - ClickHouse : 0.119s | 0.025s | 0.018s

=== READ PERFORMANCE (queries 10k records | 1k records | 100 records) ===
- [POSTGRESQL]
  - avg_likes: 0.0152s | 0.0138s | 0.0119s
  - top10_downloads: 0.0018s | 0.0006s | 0.0006s
  - modification_stats: 0.0018s | 0.0007s | 0.0005s
- [CLICKHOUSE]
  - avg_likes: 0.0055s | 0.0050s | 0.1531s
  - top10_downloads: 0.0024s | 0.0026s | 0.0025s
  - modification_stats: 0.0043s | 0.0047s | 0.0051s
- [MONGODB]
  - avg_likes: 0.0045s | 0.0033s | 0.0032s
  - top10_downloads: 0.0046s | 0.0016s | 0.0020s
  - modification_stats: 0.0047s | 0.0016s | 0.0023s
=== READ PERFORMANCE (JOIN queries) ===
- [POSTGRESQL]
  - join_modification_stats: 0.0067s | 0.0040s | 0.0051s
- [CLICKHOUSE]
  - join_modification_stats: 0.0072s | 0.0070s | 0.0143s
- [MONGODB]
  - join_modification_stats: 0.2396s | 0.0231s | 0.0085s

#### Вывод по результатам анализа
`MongoDB` быстрая (запись 0.585s, чтение 0.0032s), но без схемы и foreign keys нормализация 3НФ невозможна. Медленные JOIN (0.2396s) усложнят витрины и дашборды с отношениями между таблицами.
`ClickHouse` доминирует в записи (0.119s для 10k), но не поддерживает ACID-транзакции и foreign keys, необходимые для нормализации 3NF и обеспечения референциальной целостности. 

`PostgreSQL` идеально подходит для нормализации до 3НФ — поддерживает foreign keys и транзакции для устранения транзитивных зависимостей в models_ods, papers_ods, posts_ods. Чтение стабильно быстрое (avg_likes 0.0119s, JOIN 0.0040s), что обеспечит дашборды и витрины данных без проблем. Медленная запись (2.387s для 10k) приемлема для реализованных ETL, так как данных не должно быть больше 1000.

#### Итоговое решение
Для дальнейшей работы будет использоваться PostgreSQL как основное хранилище. Оно обеспечит полную поддержку нормализации 3НФ для лабораторных а также стабильные JOIN для витрин данных и дашбордов.