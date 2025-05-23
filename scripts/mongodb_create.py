#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo
import psycopg2
from psycopg2.extras import DictCursor
from faker import Faker
import json
from pprint import pprint

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

def connect_to_mongodb():
    """Установка соединения с MongoDB"""
    try:
        # Подключение к MongoDB без аутентификации
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        # Проверка соединения
        client.admin.command('ping')
        print("✅ Соединение с MongoDB установлено")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ Ошибка подключения к MongoDB: {str(e)}")
        return None

def create_storage(client):
    """Создание хранилища данных (базы данных и коллекции) в MongoDB"""
    # Создаем базу данных 'university'
    db = client['university']
    
    # Проверяем, существует ли коллекция 'groups'
    if 'groups' in db.list_collection_names():
        # Удаляем существующую коллекцию для чистого импорта
        db.drop_collection('groups')
        print("⚠️ Существующая коллекция 'groups' удалена")
    
    # Создаем коллекцию
    db.create_collection('groups')
    print("✅ Коллекция 'groups' создана")
    
    return db

def add_data(db, pg_cursor):
    """Добавление данных из PostgreSQL в MongoDB"""
    # Получаем данные о группах из PostgreSQL
    groups_query = """
    SELECT g.id, g.name, g.startYear, g.endYear, 
           d.id as department_id, d.name as department_name,
           i.id as institute_id, i.name as institute_name,
           u.id as university_id, u.name as university_name
    FROM groups g
    JOIN departments d ON g.id_kafedr_a = d.id
    JOIN institutes i ON d.id_institutes = i.id
    JOIN universities u ON i.id_univer = u.id
    """
    
    pg_cursor.execute(groups_query)
    groups_data = pg_cursor.fetchall()
    
    if not groups_data:
        print("⚠️ В PostgreSQL не найдены группы для импорта")
        return None
    
    # Получаем коллекцию
    groups = db['groups']
    
    # Для каждой группы получаем список её студентов и создаем документ
    for group in groups_data:
        # Получаем список студентов этой группы
        students_query = """
        SELECT s.id, s.fio, s.date_of_recipient
        FROM students s
        WHERE s.id_group = %s
        """
        
        pg_cursor.execute(students_query, (group['id'],))
        students_data = pg_cursor.fetchall()
        
        # Формируем список студентов для MongoDB
        students = []
        for student in students_data:
            students.append({
                "id": student['id'],
                "fio": student['fio'],
                "date_of_recipient": student['date_of_recipient'].strftime('%Y-%m-%d') if student['date_of_recipient'] else None
            })
        
        # Создаем документ группы с вложенным списком студентов
        group_doc = {
            "id": group['id'],
            "name": group['name'],
            "startYear": group['startyear'].strftime('%Y-%m-%d') if group['startyear'] else None,
            "endYear": group['endyear'].strftime('%Y-%m-%d') if group['endyear'] else None,
            "department": {
                "id": group['department_id'],
                "name": group['department_name']
            },
            "institute": {
                "id": group['institute_id'],
                "name": group['institute_name']
            },
            "university": {
                "id": group['university_id'],
                "name": group['university_name']
            },
            "students": students,
            "student_count": len(students)
        }
        
        # Вставляем группу в коллекцию
        result = groups.insert_one(group_doc)
        print(f"✅ Группа '{group['name']}' с {len(students)} студентами импортирована в MongoDB")
    
    print(f"✅ Всего импортировано {len(groups_data)} групп в MongoDB")
    
    # Создаем индексы для более быстрого поиска
    groups.create_index("id", unique=True)
    groups.create_index("name")
    groups.create_index("students.id")
    print("✅ Созданы индексы для оптимизации запросов")
    
    return groups_data[0]['id'] if groups_data else None

def read_sample(db, group_id=None):
    """Чтение образца данных для проверки"""
    groups = db['groups']
    
    # Получаем количество записей
    count = groups.count_documents({})
    print(f"✅ В коллекции 'groups' {count} группы(а)")
    
    # Если передан ID группы, ищем её
    if group_id:
        group = groups.find_one({"id": group_id})
    else:
        # Иначе берем любую первую группу
        group = groups.find_one()
    
    if group:
        # Преобразуем ObjectId в строку для вывода
        group_data = {"_id": str(group["_id"]), **{k: v for k, v in group.items() if k != "_id"}}
        
        # Выводим основную информацию о группе
        print(f"✅ Пример данных группы '{group['name']}':")
        print(json.dumps({
            "id": group_data["id"],
            "name": group_data["name"],
            "department": group_data["department"]["name"],
            "institute": group_data["institute"]["name"],
            "university": group_data["university"]["name"],
            "student_count": group_data["student_count"]
        }, ensure_ascii=False, indent=4))
        
        # Показываем количество студентов и примеры
        students = group.get("students", [])
        print(f"✅ В группе {len(students)} студентов")
        if students:
            print(f"✅ Пример данных первых 3 студентов:")
            for i, student in enumerate(students[:3]):
                print(f"  {i+1}. {student['fio']} (ID: {student['id']})")
    else:
        print("❌ В коллекции нет групп")
    
    # Выполняем пример агрегационного запроса: количество студентов по группам
    pipeline = [
        {"$project": {"name": 1, "student_count": 1}},
        {"$sort": {"student_count": -1}}
    ]
    
    results = list(groups.aggregate(pipeline))
    if results:
        print("\n✅ Статистика по количеству студентов в группах:")
        for i, result in enumerate(results):
            print(f"  {i+1}. Группа '{result['name']}': {result['student_count']} студентов")

def main():
    """Основная функция создания и наполнения хранилища"""
    print("\n===== СОЗДАНИЕ И НАПОЛНЕНИЕ MONGODB =====")
    
    # Устанавливаем соединение с MongoDB
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        return
    
    # Подключаемся к PostgreSQL для получения данных
    pg_connection, pg_cursor = connect_to_postgresql()
    if not pg_connection or not pg_cursor:
        return
    
    try:
        # Создаем хранилище
        db = create_storage(mongo_client)
        
        # Импортируем данные из PostgreSQL
        group_id = add_data(db, pg_cursor)
        
        # Читаем образец для проверки
        read_sample(db, group_id)
        
        print("\n===== ЗАВЕРШЕНО =====")
        print("""
Для проверки данных в MongoDB через консоль:
mongo
> use university
> db.groups.find().pretty()
> db.groups.findOne()

Пример запроса для поиска студента по имени:
> db.groups.find({"students.fio": /Иванов/})

Пример запроса для агрегации:
> db.groups.aggregate([
    {$unwind: "$students"}, 
    {$group: {_id: "$name", count: {$sum: 1}}}, 
    {$sort: {count: -1}}
  ])

Или через MongoDB Compass:
URI: mongodb://localhost:27017/
База данных: university
Коллекция: groups
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