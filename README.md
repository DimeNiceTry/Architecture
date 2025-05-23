# Демонстрация хранилищ данных

Проект содержит примеры работы с различными хранилищами данных для учебной системы университета.

## Структура проекта

```
├── docker-compose.yml      # Конфигурация Docker-контейнеров
├── STORAGE_JUSTIFICATION.md  # Описание баз данных и их применения
├── README.md               # Документация
└── scripts/                # Директория со скриптами
    ├── setup.py            # Скрипт автоматической настройки окружения
    ├── requirements.txt    # Список зависимостей Python
    ├── run_all.py          # Запуск всех демонстраций
    ├── create_all.py       # Создание и заполнение всех БД
    ├── cleanup_all.py      # Удаление данных из всех БД
    ├── redis_operations.py # Операции с Redis
    ├── redis_create.py     # Создание и заполнение Redis
    ├── redis_cleanup.py    # Очистка данных в Redis
    ├── mongodb_operations.py # Операции с MongoDB
    ├── mongodb_create.py   # Создание и заполнение MongoDB
    ├── mongodb_cleanup.py  # Очистка данных в MongoDB
    ├── neo4j_operations.py # Операции с Neo4j
    ├── neo4j_create.py     # Создание и заполнение Neo4j
    ├── neo4j_cleanup.py    # Очистка данных в Neo4j
    ├── elasticsearch_operations.py # Операции с Elasticsearch
    ├── elasticsearch_create.py # Создание и заполнение Elasticsearch
    ├── elasticsearch_cleanup.py # Очистка данных в Elasticsearch
    ├── postgresql_operations.py # Операции с PostgreSQL
    ├── postgresql_create.py # Создание и заполнение PostgreSQL
    └── postgresql_cleanup.py # Очистка данных в PostgreSQL
```

## Запуск демонстраций

### Автоматическая настройка окружения

Для автоматической настройки окружения запустите скрипт `setup.py`:

```bash
cd scripts
python setup.py
```

Этот скрипт:
- Проверит версию Python (требуется Python 3.6+)
- Установит необходимые зависимости
- Проверит доступность Docker
- Проверит и при необходимости запустит контейнеры

### Запуск создания и заполнения баз данных

Для создания схем данных и их заполнения запустите:

```bash
cd scripts
python create_all.py
```

После успешного выполнения вы можете проверить созданные данные через соответствующие клиенты:

- **Redis**: redis-cli
- **MongoDB**: mongosh или MongoDB Compass
- **Neo4j**: http://localhost:7474 (Neo4j Browser)
- **Elasticsearch**: http://localhost:9200/_cat/indices?v
- **PostgreSQL**: psql -U postgres -h localhost

### Запуск удаления данных

Для удаления данных из всех хранилищ запустите:

```bash
cd scripts
python cleanup_all.py
```

### Запуск полной демонстрации

Для запуска полной демонстрации со всеми операциями (создание, чтение, обновление, удаление):

```bash
cd scripts
python run_all.py
```

### Запуск отдельных скриптов

Вы также можете запускать отдельные скрипты для каждой базы данных:

```bash
# Создание и заполнение
python redis_create.py
python mongodb_create.py
python neo4j_create.py
python elasticsearch_create.py
python postgresql_create.py

# Полные демонстрации с CRUD операциями
python redis_operations.py
python mongodb_operations.py
python neo4j_operations.py
python elasticsearch_operations.py
python postgresql_operations.py

# Удаление данных
python redis_cleanup.py
python mongodb_cleanup.py
python neo4j_cleanup.py
python elasticsearch_cleanup.py
python postgresql_cleanup.py
```

## Устранение проблем

### Для ElasticSearch
- Проверьте, что ElasticSearch доступен по адресу http://localhost:9200
- Альтернативный порт: 9201
- Может потребоваться увеличить виртуальную память:
  ```bash
  sysctl -w vm.max_map_count=262144
  ```

### Для MongoDB
- Проверьте доступность MongoDB по адресу localhost:27017
- Скрипты настроены для работы без аутентификации

### Для Neo4j
- Проверьте доступность Neo4j Browser по адресу http://localhost:7474
- Логин/пароль по умолчанию: neo4j/neo4j123

### Для PostgreSQL
- Проверьте доступность по адресу localhost:5432
- Логин/пароль по умолчанию: postgres/postgres

### Для Redis
- Проверьте доступность Redis по адресу localhost:6379
- Для просмотра данных: `redis-cli keys *`

## Учетные данные для подключения

### MongoDB
- Пользователь: admin
- Пароль: admin123

### Neo4j
- Пользователь: neo4j
- Пароль: neo4j123

### PostgreSQL
- Пользователь: admin
- Пароль: admin123
- База данных: myapp # Architecture
