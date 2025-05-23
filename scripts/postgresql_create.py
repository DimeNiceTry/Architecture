#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import Error
from faker import Faker
import random
import datetime
from datetime import timedelta
import json

# Создаем генератор случайных данных
fake = Faker('ru_RU')
Faker.seed(42)  # Для воспроизводимости результатов
random.seed(42)

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
    except (Exception, Error) as error:
        print(f"❌ Ошибка при работе с PostgreSQL: {error}")
        return None, None

def create_schema(cursor):
    """Создание схемы для аналитической БД университета в соответствии с требуемой структурой"""
    try:
        # Сначала удаляем таблицы, если они существуют
        tables = [
            "visits", "schedule", "materials", "students", "lectures", 
            "groups", "courses", "departments", "specialties", 
            "institutes", "universities"
        ]
        
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        
        print("✅ Старые таблицы удалены (если существовали)")
        
        # 1. Создаем таблицу universities (университеты)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS universities (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
        """)

        # 2. Создаем таблицу institutes (институты)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutes (
            id SERIAL PRIMARY KEY,
            id_univer INTEGER REFERENCES universities(id),
            name VARCHAR(100) NOT NULL
        )
        """)

        # 3. Создаем таблицу departments (кафедры)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            id SERIAL PRIMARY KEY,
            id_institutes INTEGER REFERENCES institutes(id),
            name VARCHAR(100) NOT NULL
        )
        """)

        # 4. Создаем таблицу specialties (специальности)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS specialties (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            code VARCHAR(20) NOT NULL
        )
        """)

        # 5. Создаем таблицу courses (курсы)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS courses (
            id SERIAL PRIMARY KEY,
            id_kafedr_a INTEGER REFERENCES departments(id),
            id_spec INTEGER REFERENCES specialties(id),
            name VARCHAR(100) NOT NULL,
            term DATE
        )
        """)

        # 6. Создаем таблицу groups (группы)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id SERIAL PRIMARY KEY,
            id_kafedr_a INTEGER REFERENCES departments(id),
            name VARCHAR(100) NOT NULL,
            startYear DATE,
            endYear DATE
        )
        """)

        # 7. Создаем таблицу students (студенты)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            id_group INTEGER REFERENCES groups(id),
            fio VARCHAR(100) NOT NULL,
            date_of_recipient DATE
        )
        """)

        # 8. Создаем таблицу lectures (лекции)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lectures (
            id SERIAL PRIMARY KEY,
            id_course INTEGER REFERENCES courses(id),
            name VARCHAR(100) NOT NULL,
            requirements BOOLEAN DEFAULT FALSE
        )
        """)

        # 9. Создаем таблицу materials (материалы)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS materials (
            id SERIAL PRIMARY KEY,
            id_lect INTEGER REFERENCES lectures(id),
            name VARCHAR(100) NOT NULL,
            content TEXT
        )
        """)

        # 10. Создаем таблицу schedule (расписание)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schedule (
            id SERIAL PRIMARY KEY,
            id_lect INTEGER REFERENCES lectures(id),
            id_group INTEGER REFERENCES groups(id),
            startTime TIMESTAMP WITH TIME ZONE,
            endTime TIMESTAMP WITH TIME ZONE
        )
        """)

        # 11. Создаем таблицу visits (посещения)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            id SERIAL PRIMARY KEY,
            id_student INTEGER REFERENCES students(id),
            id_rasp INTEGER REFERENCES schedule(id),
            visitTime TIMESTAMP WITH TIME ZONE
        )
        """)

        print("✅ Схема данных успешно создана")
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при создании схемы: {error}")
        return False

def add_data(cursor):
    """Добавление тестовых данных в таблицы"""
    try:
        # 1. Добавляем университеты
        universities = [
            ('Московский Государственный Университет'),
            ('Санкт-Петербургский Государственный Университет'),
            ('Казанский Федеральный Университет')
        ]
        
        for university in universities:
            cursor.execute(
                "INSERT INTO universities (name) VALUES (%s) RETURNING id",
                (university,)
            )
            university_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(universities)} университетов")
        
        # 2. Добавляем институты
        institutes = [
            (1, 'Институт компьютерных наук'),
            (1, 'Институт математики'),
            (2, 'Институт информационных технологий'),
            (3, 'Институт физики')
        ]
        
        for institute in institutes:
            cursor.execute(
                "INSERT INTO institutes (id_univer, name) VALUES (%s, %s) RETURNING id",
                institute
            )
            institute_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(institutes)} институтов")
        
        # 3. Добавляем кафедры
        departments = [
            (1, 'Кафедра программной инженерии'),
            (1, 'Кафедра баз данных'),
            (2, 'Кафедра высшей математики'),
            (3, 'Кафедра искусственного интеллекта'),
            (4, 'Кафедра теоретической физики')
        ]
        
        for dept in departments:
            cursor.execute(
                "INSERT INTO departments (id_institutes, name) VALUES (%s, %s) RETURNING id",
                dept
            )
            dept_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(departments)} кафедр")
        
        # 4. Добавляем специальности
        specialties = [
            ('Информатика и вычислительная техника', '09.03.01'),
            ('Программная инженерия', '09.03.04'),
            ('Прикладная математика', '01.03.04'),
            ('Информационная безопасность', '10.03.01')
        ]
        
        for spec in specialties:
            cursor.execute(
                "INSERT INTO specialties (name, code) VALUES (%s, %s) RETURNING id",
                spec
            )
            spec_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(specialties)} специальностей")

        # 5. Добавляем курсы
        courses = [
            (1, 1, 'Введение в программирование', '2023-09-01'),
            (2, 1, 'Базы данных', '2023-09-01'),
            (3, 3, 'Математический анализ', '2023-09-01'),
            (4, 2, 'Машинное обучение', '2023-09-01'),
            (5, 4, 'Физика', '2023-09-01')
        ]
        
        for course in courses:
            cursor.execute(
                "INSERT INTO courses (id_kafedr_a, id_spec, name, term) VALUES (%s, %s, %s, %s) RETURNING id",
                course
            )
            course_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(courses)} курсов")

        # 6. Добавляем группы
        groups = [
            (1, 'ПИ-101', '2020-09-01', '2024-06-30'),
            (2, 'БД-102', '2020-09-01', '2024-06-30'),
            (3, 'МА-201', '2021-09-01', '2025-06-30'),
            (4, 'ИИ-301', '2022-09-01', '2026-06-30')
        ]
        
        for group in groups:
            cursor.execute(
                "INSERT INTO groups (id_kafedr_a, name, startYear, endYear) VALUES (%s, %s, %s, %s) RETURNING id",
                group
            )
            group_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(groups)} групп")

        # 7. Добавляем студентов
        students = []
        for i in range(1, 20):  # 20 студентов
            group_id = random.randint(1, 4)
            student = (
                group_id,
                fake.name(),
                fake.date_between(start_date='-5y', end_date='today')
            )
            students.append(student)
        
        for student in students:
            cursor.execute(
                """INSERT INTO students (id_group, fio, date_of_recipient) VALUES (%s, %s, %s) RETURNING id""",
                student
            )
            student_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(students)} студентов")

        # 8. Добавляем лекции
        lectures = [
            (1, 'Введение в алгоритмы', True),
            (1, 'Основы синтаксиса', True),
            (2, 'Реляционная модель', True),
            (2, 'SQL и нормализация', True),
            (3, 'Пределы и производные', True),
            (4, 'Нейронные сети', False),
            (5, 'Механика', True)
        ]
        
        for lecture in lectures:
            cursor.execute(
                """INSERT INTO lectures (id_course, name, requirements) VALUES (%s, %s, %s) RETURNING id""",
                lecture
            )
            lecture_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(lectures)} лекций")

        # 9. Добавляем материалы
        materials = [
            (1, 'Слайды по введению в алгоритмы', 'Содержимое слайдов по введению в алгоритмы'),
            (1, 'Примеры кода', 'Примеры алгоритмов на Python'),
            (2, 'Основы синтаксиса', 'Базовые конструкции языка'),
            (3, 'Презентация по БД', 'Основные понятия и определения СУБД'),
            (5, 'Формулы и теоремы', 'Основные формулы математического анализа')
        ]
        
        for material in materials:
            cursor.execute(
                """INSERT INTO materials (id_lect, name, content) VALUES (%s, %s, %s) RETURNING id""",
                material
            )
            material_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(materials)} материалов")

        # 10. Добавляем расписание
        now = datetime.datetime.now()
        schedules = []
        for i in range(10):
            lecture_id = random.randint(1, 7)
            group_id = random.randint(1, 4)
            start_time = now + timedelta(days=i, hours=random.randint(9, 16))
            end_time = start_time + timedelta(hours=1, minutes=30)
            
            schedules.append((
                lecture_id,
                group_id,
                start_time,
                end_time
            ))
        
        for schedule in schedules:
            cursor.execute(
                """INSERT INTO schedule (id_lect, id_group, startTime, endTime) VALUES (%s, %s, %s, %s) RETURNING id""",
                schedule
            )
            schedule_id = cursor.fetchone()['id']
        
        print(f"✅ Добавлено {len(schedules)} записей расписания")

        # 11. Добавляем посещения
        visits = []
        for i in range(30):  # 30 записей о посещениях
            student_id = random.randint(1, len(students))
            schedule_id = random.randint(1, len(schedules))
            
            # Получаем время начала занятия
            cursor.execute("SELECT startTime FROM schedule WHERE id = %s", (schedule_id,))
            start_time = cursor.fetchone()['starttime']
            
            # Генерируем время визита (либо вовремя, либо с небольшим опозданием)
            delay = random.randint(0, 15)  # Опоздание от 0 до 15 минут
            visit_time = start_time + timedelta(minutes=delay)
            
            visits.append((
                student_id,
                schedule_id,
                visit_time
            ))
        
        for visit in visits:
            cursor.execute(
                """INSERT INTO visits (id_student, id_rasp, visitTime) VALUES (%s, %s, %s)""",
                visit
            )
        
        print(f"✅ Добавлено {len(visits)} записей о посещениях")
        
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при добавлении данных: {error}")
        return False

def read_sample_data(cursor):
    """Чтение образцов данных из каждой таблицы"""
    try:
        # 1. Данные о университетах
        cursor.execute("SELECT * FROM universities LIMIT 2")
        universities = cursor.fetchall()
        print("\n== Образцы университетов ==")
        for university in universities:
            print(f"ID: {university['id']}, Название: {university['name']}")
        
        # 2. Данные о институтах
        cursor.execute("""
        SELECT i.*, u.name as university_name 
        FROM institutes i 
        JOIN universities u ON i.id_univer = u.id 
        LIMIT 2
        """)
        institutes = cursor.fetchall()
        print("\n== Образцы институтов ==")
        for institute in institutes:
            print(f"ID: {institute['id']}, Название: {institute['name']}, Университет: {institute['university_name']}")
        
        # 3. Данные о кафедрах
        cursor.execute("""
        SELECT d.*, i.name as institute_name 
        FROM departments d 
        JOIN institutes i ON d.id_institutes = i.id 
        LIMIT 3
        """)
        departments = cursor.fetchall()
        print("\n== Образцы кафедр ==")
        for dept in departments:
            print(f"ID: {dept['id']}, Название: {dept['name']}, Институт: {dept['institute_name']}")
        
        # 4. Данные о студентах
        cursor.execute("""
        SELECT s.*, g.name as group_name 
        FROM students s 
        JOIN groups g ON s.id_group = g.id 
        LIMIT 3
        """)
        students = cursor.fetchall()
        print("\n== Образцы студентов ==")
        for student in students:
            print(f"ID: {student['id']}, ФИО: {student['fio']}, Группа: {student['group_name']}, Дата: {student['date_of_recipient']}")
        
        # 5. Статистика
        cursor.execute("SELECT COUNT(*) as universities_count FROM universities")
        universities_count = cursor.fetchone()['universities_count']
        
        cursor.execute("SELECT COUNT(*) as institutes_count FROM institutes")
        institutes_count = cursor.fetchone()['institutes_count']
        
        cursor.execute("SELECT COUNT(*) as departments_count FROM departments")
        departments_count = cursor.fetchone()['departments_count']
        
        cursor.execute("SELECT COUNT(*) as specialties_count FROM specialties")
        specialties_count = cursor.fetchone()['specialties_count']
        
        cursor.execute("SELECT COUNT(*) as courses_count FROM courses")
        courses_count = cursor.fetchone()['courses_count']
        
        cursor.execute("SELECT COUNT(*) as groups_count FROM groups")
        groups_count = cursor.fetchone()['groups_count']
        
        cursor.execute("SELECT COUNT(*) as students_count FROM students")
        students_count = cursor.fetchone()['students_count']
        
        cursor.execute("SELECT COUNT(*) as lectures_count FROM lectures")
        lectures_count = cursor.fetchone()['lectures_count']
        
        cursor.execute("SELECT COUNT(*) as materials_count FROM materials")
        materials_count = cursor.fetchone()['materials_count']
        
        cursor.execute("SELECT COUNT(*) as schedule_count FROM schedule")
        schedule_count = cursor.fetchone()['schedule_count']
        
        cursor.execute("SELECT COUNT(*) as visits_count FROM visits")
        visits_count = cursor.fetchone()['visits_count']
        
        print("\n== Статистика ==")
        print(f"Университетов: {universities_count}")
        print(f"Институтов: {institutes_count}")
        print(f"Кафедр: {departments_count}")
        print(f"Специальностей: {specialties_count}")
        print(f"Курсов: {courses_count}")
        print(f"Групп: {groups_count}")
        print(f"Студентов: {students_count}")
        print(f"Лекций: {lectures_count}")
        print(f"Материалов: {materials_count}")
        print(f"Записей расписания: {schedule_count}")
        print(f"Записей посещений: {visits_count}")
        
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при чтении данных: {error}")
        return False

def main():
    """Основная функция управления демонстрацией PostgreSQL"""
    print("\n===== СОЗДАНИЕ И ЗАПОЛНЕНИЕ POSTGRESQL =====")
    
    # Подключаемся к PostgreSQL
    connection, cursor = connect_to_postgresql()
    if not connection or not cursor:
        return
        
    try:
        # Создаем схему БД
        success = create_schema(cursor)
        if not success:
            return
            
        # Заполняем данными
        success = add_data(cursor)
        if not success:
            return
            
        # Читаем данные
        read_sample_data(cursor)
            
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
    finally:
        # Закрываем соединение
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("✅ Соединение с PostgreSQL закрыто")
    
    print("\n===== ОПЕРАЦИИ С POSTGRESQL ЗАВЕРШЕНЫ =====")
    print("\nДля проверки данных в PostgreSQL вы можете использовать:")
    print("1. Утилиту psql: psql -U postgres -h localhost")
    print("2. pgAdmin 4 или другой GUI-клиент")
    print("3. SQL-запросы вроде: SELECT * FROM students LIMIT 5;")

if __name__ == "__main__":
    main() 