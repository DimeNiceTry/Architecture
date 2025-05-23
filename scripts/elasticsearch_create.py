#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from faker import Faker
import json
import time
import psycopg2
from psycopg2.extras import DictCursor

# Инициализация генератора случайных данных
fake = Faker('ru_RU')

def connect_to_postgresql():
    """Подключение к PostgreSQL"""
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432",
            database="postgres"
        )
        connection.autocommit = True

        # Создаем объект курсора для выполнения операций с базой данных
        cursor = connection.cursor(cursor_factory=DictCursor)
        print("✅ Соединение с PostgreSQL установлено")
        return connection, cursor
    except Exception as error:
        print(f"❌ Ошибка при работе с PostgreSQL: {error}")
        return None, None

def connect_to_elasticsearch():
    """Установка соединения с Elasticsearch"""
    try:
        # Пробуем различные варианты подключения
        # Сначала по порту из docker-compose.yml
        es = Elasticsearch("http://localhost:9200")
        if es.ping():
            print("✅ Соединение с Elasticsearch установлено по порту 9200")
            return es
        
        # Если не удалось, пробуем альтернативный порт, указанный в README
        es = Elasticsearch("http://localhost:9201")
        if es.ping():
            print("✅ Соединение с Elasticsearch установлено по порту 9201")
            return es
            
        print("❌ Ошибка подключения к Elasticsearch - сервер не отвечает")
        return None
    except Exception as e:
        print(f"❌ Ошибка подключения к Elasticsearch: {str(e)}")
        return None

def create_storage(es):
    """Создание индексов для хранения данных о курсах и материалах"""
    try:
        # Создаем индекс для материалов с полнотекстовым поиском
        index_name = "materials"
        
        # Сначала проверяем, существует ли индекс
        if es.indices.exists(index=index_name):
            print(f"⚠️ Индекс '{index_name}' уже существует и будет пересоздан")
            es.indices.delete(index=index_name)
        
        # Пробуем создать индекс с разными версиями API
        try:
            # Попытка с body параметром (старые версии ES)
            mapping = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "russian"},
                        "content": {"type": "text", "analyzer": "russian"},
                        "lecture_id": {"type": "keyword"},
                        "lecture_name": {"type": "text", "analyzer": "russian"},
                        "course_id": {"type": "keyword"},
                        "course_name": {"type": "text", "analyzer": "russian"},
                        "department_id": {"type": "keyword"},
                        "department_name": {"type": "text", "analyzer": "russian"},
                        "created_at": {"type": "date"}
                    }
                },
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "russian": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "russian_stop", "russian_stemmer"]
                            }
                        },
                        "filter": {
                            "russian_stop": {
                                "type": "stop",
                                "stopwords": "_russian_"
                            },
                            "russian_stemmer": {
                                "type": "stemmer",
                                "language": "russian"
                            }
                        }
                    }
                }
            }
            es.indices.create(index=index_name, body=mapping)
            print(f"✅ Индекс '{index_name}' создан в Elasticsearch (метод body)")
        except TypeError:
            # Попытка с новым API (ES 8.x)
            mappings = {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "russian"},
                    "content": {"type": "text", "analyzer": "russian"},
                    "lecture_id": {"type": "keyword"},
                    "lecture_name": {"type": "text", "analyzer": "russian"},
                    "course_id": {"type": "keyword"},
                    "course_name": {"type": "text", "analyzer": "russian"},
                    "department_id": {"type": "keyword"},
                    "department_name": {"type": "text", "analyzer": "russian"},
                    "created_at": {"type": "date"}
                }
            }
            settings = {
                "analysis": {
                    "analyzer": {
                        "russian": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "russian_stop", "russian_stemmer"]
                        }
                    },
                    "filter": {
                        "russian_stop": {
                            "type": "stop",
                            "stopwords": "_russian_"
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        }
                    }
                }
            }
            es.indices.create(index=index_name, mappings=mappings, settings=settings)
            print(f"✅ Индекс '{index_name}' создан в Elasticsearch (метод mappings)")
    except Exception as e:
        print(f"❌ Ошибка создания индекса: {str(e)}")
        
        # Упрощенный вариант для проблемных версий ES
        try:
            if es.indices.exists(index=index_name):
                es.indices.delete(index=index_name)
            
            # Пробуем без маппинга
            es.indices.create(index=index_name)
            print(f"✅ Индекс '{index_name}' создан без маппинга")
        except Exception as e2:
            print(f"❌ Критическая ошибка создания индекса: {str(e2)}")
            return False
    
    return True

def add_data(es, pg_cursor):
    """Добавление данных из PostgreSQL в Elasticsearch"""
    # Получаем материалы с дополнительной информацией из PostgreSQL
    query = """
    SELECT m.id, m.name, m.content, 
           l.id as lecture_id, l.name as lecture_name,
           c.id as course_id, c.name as course_name,
           d.id as department_id, d.name as department_name
    FROM materials m
    JOIN lectures l ON m.id_lect = l.id
    JOIN courses c ON l.id_course = c.id
    JOIN departments d ON c.id_kafedr_a = d.id
    """
    
    pg_cursor.execute(query)
    materials = pg_cursor.fetchall()
    
    if not materials:
        print("⚠️ В PostgreSQL не найдены материалы для импорта")
        return None
    
    # Индексируем материалы в Elasticsearch
    current_time = int(time.time())
    indexed_count = 0
    
    for material in materials:
        # Создаем документ для индексации
        doc = {
            "id": material['id'],
            "name": material['name'],
            "content": material['content'] or f"Содержимое материала {material['name']}",
            "lecture_id": material['lecture_id'],
            "lecture_name": material['lecture_name'],
            "course_id": material['course_id'],
            "course_name": material['course_name'],
            "department_id": material['department_id'],
            "department_name": material['department_name'],
            "created_at": current_time
        }
        
        try:
            # Добавляем документ в Elasticsearch с обработкой разных версий API
            try:
                # В новых версиях ES используется document
                es.index(index="materials", id=str(material['id']), document=doc)
            except TypeError:
                # В более старых версиях ES используется body
                es.index(index="materials", id=str(material['id']), body=doc)
            
            indexed_count += 1
        except Exception as e:
            print(f"❌ Ошибка индексации материала {material['id']}: {str(e)}")
    
    # Обновляем индекс для немедленной доступности данных
    try:
        es.indices.refresh(index="materials")
    except Exception as e:
        print(f"⚠️ Ошибка обновления индекса: {str(e)}")
    
    print(f"✅ В Elasticsearch индексировано {indexed_count} материалов из {len(materials)}")
    
    return materials[0]['id'] if materials else None

def check_index_status(es):
    """Проверка статуса индекса и количества документов"""
    try:
        # Проверяем существование индекса
        if not es.indices.exists(index="materials"):
            print("❌ Индекс 'materials' не существует")
            return False
        
        # Получаем статистику индекса
        try:
            # ES 7.x+
            count = es.count(index="materials")["count"]
        except TypeError:
            # ES 8.x+
            count = es.count(index="materials").get("count", 0)
        
        print(f"✅ Индекс 'materials' содержит {count} документов")
        return count > 0
    except Exception as e:
        print(f"❌ Ошибка проверки индекса: {str(e)}")
        return False

def test_search(es):
    """Тестирование полнотекстового поиска"""
    try:
        # Поиск материалов по различным ключевым словам
        keywords = ["алгоритм", "данные", "программирование", "анализ", "база"]
        
        for keyword in keywords:
            print(f"\n✅ Поиск материалов по ключевому слову '{keyword}':")
            query = {
                "query": {
                    "multi_match": {
                        "query": keyword,
                        "fields": ["name^3", "content^2", "lecture_name", "course_name"]
                    }
                },
                "highlight": {
                    "fields": {
                        "name": {},
                        "content": {},
                        "lecture_name": {},
                        "course_name": {}
                    }
                }
            }
            
            try:
                # ES 7.x
                results = es.search(index="materials", body=query, size=3)
            except TypeError:
                # ES 8.x
                results = es.search(index="materials", query=query["query"], highlight=query["highlight"], size=3)
            
            hits = results.get("hits", {}).get("hits", [])
            if hits:
                print(f"  Найдено {len(hits)} результатов:")
                for i, hit in enumerate(hits):
                    source = hit.get("_source", {})
                    score = hit.get("_score")
                    highlights = hit.get("highlight", {})
                    
                    print(f"  {i+1}. {source.get('name')} (курс: {source.get('course_name')}) - релевантность: {score:.2f}")
                    
                    if highlights:
                        if "content" in highlights:
                            print(f"     Фрагмент: {' ... '.join(highlights['content'])[:100]}...")
            else:
                print(f"  По запросу '{keyword}' ничего не найдено")
        
        return True
    except Exception as e:
        print(f"❌ Ошибка поиска: {str(e)}")
        return False

def main():
    """Основная функция создания и наполнения хранилища"""
    print("\n===== СОЗДАНИЕ И НАПОЛНЕНИЕ ELASTICSEARCH =====")
    
    # Устанавливаем соединение с Elasticsearch
    es = connect_to_elasticsearch()
    if not es:
        return
    
    # Подключаемся к PostgreSQL для получения данных
    pg_connection, pg_cursor = connect_to_postgresql()
    if not pg_connection or not pg_cursor:
        return
    
    # Создаем индекс
    if not create_storage(es):
        print("❌ Не удалось создать индекс. Прерываем выполнение.")
        pg_connection.close()
        return
    
    try:
        # Импортируем данные из PostgreSQL
        add_data(es, pg_cursor)
        
        # Проверяем статус индекса
        check_index_status(es)
        
        # Тестируем поиск
        test_search(es)
        
        print("\n===== ЗАВЕРШЕНО =====")
        print("""
Для проверки данных в Elasticsearch:

1. Проверка работы Elasticsearch:
   curl -XGET 'http://localhost:9200/_cat/indices?v'

2. Просмотр всех материалов:
   curl -XGET 'http://localhost:9200/materials/_search?pretty'

3. Поиск по ключевым словам:
   curl -XGET 'http://localhost:9200/materials/_search?q=алгоритм&pretty'

4. Более сложный поиск с весами полей:
   curl -XPOST 'http://localhost:9200/materials/_search?pretty' -H 'Content-Type: application/json' -d '
   {
     "query": {
       "multi_match": {
         "query": "алгоритм",
         "fields": ["name^3", "content^2", "course_name", "lecture_name"]
       }
     }
   }'

5. Через Kibana (если установлена):
   http://localhost:5601
   
   Затем создать индексный паттерн для materials и использовать Discover.
        """)
    finally:
        # Закрываем соединения
        if pg_cursor:
            pg_cursor.close()
        if pg_connection:
            pg_connection.close()
            print("✅ Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main() 