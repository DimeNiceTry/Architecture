#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import json
from faker import Faker
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

def connect_to_redis():
    """Установка соединения с Redis"""
    try:
        # Подключение к Redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        # Проверка соединения
        r.ping()
        print("✅ Соединение с Redis установлено")
        return r
    except redis.ConnectionError as e:
        print(f"❌ Ошибка подключения к Redis: {str(e)}")
        return None

def create_storage(r):
    """Создание хранилища данных в Redis"""
    # Очистка предыдущих данных (если есть)
    students_keys = r.keys("student:*")
    if students_keys:
        r.delete(*students_keys)
        print(f"⚠️ Удалено {len(students_keys)} существующих ключей студентов")
    
    if r.exists("students:all"):
        r.delete("students:all")
        print("⚠️ Удален существующий набор ID студентов")
    
    # Создаем ключ-метку для проверки наличия хранилища
    r.set("students:info", "Список студентов из центральной PostgreSQL БД")
    print("✅ Хранилище для студентов создано")

def import_student_data(r, pg_cursor):
    """Импорт данных о студентах из PostgreSQL в Redis"""
    # Получаем полную информацию о студентах с присоединенными таблицами
    query = """
    SELECT s.id, s.fio, s.date_of_recipient,
           g.id as group_id, g.name as group_name,
           d.id as department_id, d.name as department_name,
           i.id as institute_id, i.name as institute_name,
           u.id as university_id, u.name as university_name
    FROM students s
    JOIN groups g ON s.id_group = g.id
    JOIN departments d ON g.id_kafedr_a = d.id
    JOIN institutes i ON d.id_institutes = i.id
    JOIN universities u ON i.id_univer = u.id
    """
    
    pg_cursor.execute(query)
    students = pg_cursor.fetchall()
    
    if not students:
        print("⚠️ В PostgreSQL не найдены студенты для импорта")
        return None
    
    # Импортируем каждого студента в Redis
    student_ids = []
    for student in students:
        student_id = student['id']
        student_ids.append(str(student_id))
        
        # Формируем детальные данные о студенте
        student_data = {
            "id": student_id,
            "fio": student['fio'],
            "date_of_recipient": student['date_of_recipient'].strftime('%Y-%m-%d') if student['date_of_recipient'] else None,
            "group": {
                "id": student['group_id'],
                "name": student['group_name']
            },
            "department": {
                "id": student['department_id'],
                "name": student['department_name']
            },
            "institute": {
                "id": student['institute_id'],
                "name": student['institute_name']
            },
            "university": {
                "id": student['university_id'],
                "name": student['university_name']
            }
        }
        
        # Сохраняем в Redis
        r.set(f"student:{student_id}", json.dumps(student_data, ensure_ascii=False))
    
    # Сохраняем список всех ID студентов для удобства поиска
    if student_ids:
        r.sadd("students:all", *student_ids)
    
    print(f"✅ Импортировано {len(student_ids)} студентов в Redis")
    
    # Создаем дополнительные индексы для быстрого поиска
    # Например, индекс по группам
    for student in students:
        group_id = student['group_id']
        student_id = student['id']
        r.sadd(f"group:{group_id}:students", student_id)
    
    print("✅ Созданы дополнительные индексы для поиска студентов по группам")
    
    return students[0]['id'] if students else None

def import_visit_data(r, pg_cursor):
    """Импорт данных о посещениях в Redis для быстрого кэширования"""
    # Получаем данные о посещениях
    query = """
    SELECT v.id, v.id_student, v.id_rasp, v.visitTime,
           s.id_group, s.id_lect, 
           l.name as lecture_name
    FROM visits v
    JOIN schedule s ON v.id_rasp = s.id
    JOIN lectures l ON s.id_lect = l.id
    """
    
    pg_cursor.execute(query)
    visits = pg_cursor.fetchall()
    
    if not visits:
        print("⚠️ В PostgreSQL не найдены посещения для импорта")
        return
    
    # Для каждого студента создаем упорядоченный список его посещений (sorted set)
    visit_count = 0
    for visit in visits:
        student_id = visit['id_student']
        lecture_id = visit['id_lect']
        visit_time = visit['visittime'].timestamp() if visit['visittime'] else 0
        
        visit_data = {
            "id": visit['id'],
            "schedule_id": visit['id_rasp'],
            "lecture_id": lecture_id,
            "lecture_name": visit['lecture_name'],
            "visit_time": str(visit['visittime'])
        }
        
        # Добавляем посещение в упорядоченный список студента
        r.zadd(f"student:{student_id}:visits", {json.dumps(visit_data, ensure_ascii=False): visit_time})
        
        # Добавляем ID студента в множество посетивших лекцию
        r.sadd(f"lecture:{lecture_id}:visitors", student_id)
        
        visit_count += 1
    
    print(f"✅ Импортировано {visit_count} посещений в Redis")

def read_sample(r, student_id=None):
    """Чтение образца данных для проверки"""
    # Получаем все ID студентов
    student_ids = list(r.smembers("students:all"))
    if not student_ids:
        print("❌ В базе нет данных о студентах")
        return
    
    # Если передан ID, используем его, иначе берем первый из списка
    if student_id and str(student_id) in student_ids:
        first_id = str(student_id)
    else:
        first_id = student_ids[0]
    
    # Получаем данные студента
    student_data = r.get(f"student:{first_id}")
    if student_data:
        student = json.loads(student_data)
        print(f"✅ Данные студента с ID {first_id}:")
        print(f"  ФИО: {student['fio']}")
        print(f"  Группа: {student['group']['name']}")
        print(f"  Кафедра: {student['department']['name']}")
        print(f"  Институт: {student['institute']['name']}")
        print(f"  Университет: {student['university']['name']}")
        
        # Показываем посещения студента (если есть)
        visits = r.zrange(f"student:{first_id}:visits", 0, -1)
        if visits:
            print(f"✅ Список посещений ({len(visits)}):")
            for i, visit_json in enumerate(visits[:3]):  # Выводим только первые 3 посещения
                visit = json.loads(visit_json)
                print(f"  {i+1}. Лекция: {visit['lecture_name']}, Время: {visit['visit_time']}")
            
            if len(visits) > 3:
                print(f"  ... и еще {len(visits) - 3} записей")
        else:
            print("⚠️ У студента нет записей о посещениях")
        
        # Показываем все ключи в Redis для студентов
        all_keys = r.keys("student:*")
        print(f"\n✅ Всего записей о студентах в Redis: {len(all_keys)}")
    else:
        print(f"❌ Данные студента с ID {first_id} не найдены")
    
    # Проверяем наличие индексов по группам
    group_keys = r.keys("group:*:students")
    if group_keys:
        print(f"✅ Созданы индексы для {len(group_keys)} групп")
        
        # Берем первую группу как пример
        first_group_key = group_keys[0]
        group_id = first_group_key.split(':')[1]
        members = r.smembers(first_group_key)
        print(f"  Группа {group_id} содержит {len(members)} студентов")

def main():
    """Основная функция создания и наполнения хранилища"""
    print("\n===== СОЗДАНИЕ И НАПОЛНЕНИЕ REDIS =====")
    
    # Устанавливаем соединение с Redis
    r = connect_to_redis()
    if not r:
        return
    
    # Подключаемся к PostgreSQL для получения данных
    pg_connection, pg_cursor = connect_to_postgresql()
    if not pg_connection or not pg_cursor:
        return
    
    try:
        # Создаем хранилище
        create_storage(r)
        
        # Импортируем данные студентов
        student_id = import_student_data(r, pg_cursor)
        
        # Импортируем данные о посещениях
        import_visit_data(r, pg_cursor)
        
        # Читаем образец для проверки
        read_sample(r, student_id)
        
        print("\n===== ЗАВЕРШЕНО =====")
        print("""
Для проверки данных в Redis через консоль:
redis-cli

> KEYS *
> GET student:1
> SMEMBERS students:all
> ZRANGE student:1:visits 0 -1
> SMEMBERS group:1:students
> SMEMBERS lecture:1:visitors

Примеры полезных запросов:
> SCARD students:all          # Получить общее количество студентов
> SCARD group:1:students      # Получить количество студентов в группе 1
> ZCARD student:1:visits      # Получить количество посещений студента 1
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