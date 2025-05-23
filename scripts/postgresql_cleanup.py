#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import Error
import json

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

def check_data(cursor):
    """Проверка наличия данных в базе"""
    try:
        tables = ["faculties", "departments", "students", "courses", "teachers", "schedule", "attendance"]
        data_exists = False
        
        print("\n== Проверка данных в таблицах ==")
        
        for table in tables:
            # Проверяем существование таблицы
            cursor.execute(f"SELECT to_regclass('public.{table}')")
            if cursor.fetchone()[0] is None:
                print(f"❌ Таблица '{table}' не существует")
                continue
                
            # Проверяем наличие данных в таблице
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"✅ Таблица '{table}': найдено {count} записей")
                data_exists = True
            else:
                print(f"⚠️ Таблица '{table}': данных нет")
        
        return data_exists
    except (Exception, Error) as error:
        print(f"❌ Ошибка при проверке данных: {error}")
        return False

def show_data_sample(cursor):
    """Показать образец данных перед удалением"""
    try:
        # Примеры студентов
        cursor.execute("""
        SELECT s.id, s.full_name, d.name as department_name, s.year
        FROM students s
        JOIN departments d ON s.department_id = d.id
        LIMIT 3
        """)
        
        students = cursor.fetchall()
        if students:
            print("\n== Примеры студентов для удаления ==")
            for student in students:
                print(f"  - ID: {student['id']}, Имя: {student['full_name']}")
                print(f"    Кафедра: {student['department_name']}, Курс: {student['year']}")
        
        # Примеры кафедр
        cursor.execute("""
        SELECT d.id, d.name, f.name as faculty_name, d.head
        FROM departments d
        JOIN faculties f ON d.faculty_id = f.id
        LIMIT 2
        """)
        
        departments = cursor.fetchall()
        if departments:
            print("\n== Примеры кафедр для удаления ==")
            for dept in departments:
                print(f"  - ID: {dept['id']}, Название: {dept['name']}")
                print(f"    Факультет: {dept['faculty_name']}, Заведующий: {dept['head']}")
        
        # Примеры расписания
        cursor.execute("""
        SELECT s.id, c.name as course_name, t.full_name as teacher_name, 
               s.room, s.day_of_week, s.start_time, s.end_time
        FROM schedule s
        JOIN courses c ON s.course_id = c.id
        JOIN teachers t ON s.teacher_id = t.id
        LIMIT 2
        """)
        
        schedule = cursor.fetchall()
        if schedule:
            day_names = ['', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            print("\n== Примеры расписания для удаления ==")
            for item in schedule:
                day = day_names[item['day_of_week']] if 1 <= item['day_of_week'] <= 7 else 'Неизвестный день'
                print(f"  - ID: {item['id']}, Курс: {item['course_name']}")
                print(f"    Преподаватель: {item['teacher_name']}, Аудитория: {item['room']}")
                print(f"    День: {day}, Время: {item['start_time']} - {item['end_time']}")
                
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при получении образцов данных: {error}")
        return False

def delete_table_data(cursor, table_name):
    """Удаление данных из определенной таблицы"""
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        print(f"✅ Данные из таблицы '{table_name}' удалены")
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при удалении данных из таблицы '{table_name}': {error}")
        return False

def delete_all_data(cursor):
    """Удаление всех данных из всех таблиц с учетом зависимостей"""
    try:
        # Удаляем данные в правильном порядке (с учетом зависимостей)
        tables_in_order = [
            "attendance",     # Сначала удаляем данные из зависимых таблиц
            "schedule",
            "students",
            "teachers",
            "courses",
            "departments",
            "faculties"
        ]
        
        for table in tables_in_order:
            delete_table_data(cursor, table)
            
        print("\n✅ Все данные успешно удалены")
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при удалении всех данных: {error}")
        return False

def drop_all_tables(cursor):
    """Удаление всех таблиц"""
    try:
        # Сначала удаляем партиции таблицы attendance
        cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename LIKE 'attendance_week_%'
        """)
        
        partition_tables = [row['tablename'] for row in cursor.fetchall()]
        
        for table in partition_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"✅ Партиция '{table}' удалена")
            
        # Удаляем основные таблицы в правильном порядке
        tables_in_order = [
            "attendance",     # Сначала удаляем зависимые таблицы
            "schedule",
            "students",
            "teachers",
            "courses",
            "departments",
            "faculties"
        ]
        
        for table in tables_in_order:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"✅ Таблица '{table}' удалена")
            
        print("\n✅ Все таблицы успешно удалены")
        return True
    except (Exception, Error) as error:
        print(f"❌ Ошибка при удалении таблиц: {error}")
        return False

def main():
    """Основная функция очистки данных PostgreSQL"""
    print("\n===== УДАЛЕНИЕ ДАННЫХ POSTGRESQL =====")
    
    # Подключаемся к PostgreSQL
    connection, cursor = connect_to_postgresql()
    if not connection or not cursor:
        return
        
    try:
        # Проверяем наличие данных
        data_exists = check_data(cursor)
        
        if not data_exists:
            print("\n⚠️ В базе данных отсутствуют данные или таблицы")
            print("\nВыберите действие:")
            print("1. Удалить все таблицы, если они существуют")
            print("2. Отмена")
            
            choice = input("Введите номер действия (1-2): ")
            
            if choice == '1':
                drop_all_tables(cursor)
            else:
                print("❌ Операция удаления отменена.")
            return
        
        # Показываем образцы данных
        show_data_sample(cursor)
        
        # Спрашиваем, что именно нужно удалить
        print("\nВыберите действие:")
        print("1. Удалить только данные из таблиц (структуру сохранить)")
        print("2. Удалить таблицы полностью (вместе с данными)")
        print("3. Отмена")
        
        choice = input("Введите номер действия (1-3): ")
        
        if choice == '1':
            confirm = input("Вы точно хотите удалить ВСЕ данные из таблиц? (y/n): ")
            if confirm.lower() == 'y':
                delete_all_data(cursor)
            else:
                print("❌ Операция удаления отменена.")
        elif choice == '2':
            confirm = input("Вы точно хотите удалить ВСЕ таблицы вместе с данными? (y/n): ")
            if confirm.lower() == 'y':
                drop_all_tables(cursor)
            else:
                print("❌ Операция удаления отменена.")
        else:
            print("❌ Операция удаления отменена.")
            
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
    finally:
        # Закрываем соединение
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("✅ Соединение с PostgreSQL закрыто")
    
    print("\n===== УДАЛЕНИЕ POSTGRESQL ЗАВЕРШЕНО =====")

if __name__ == "__main__":
    main() 