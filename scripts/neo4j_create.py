#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j import GraphDatabase
from faker import Faker
import json
import psycopg2
from psycopg2.extras import DictCursor

# Инициализация генератора случайных данных
fake = Faker('ru_RU')

class Neo4jDemo:
    def __init__(self, uri, user, password):
        """Инициализация драйвера Neo4j"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        """Закрытие соединения"""
        if self.driver:
            self.driver.close()
    
    def run_query(self, query, **params):
        """Выполнение запроса к Neo4j"""
        with self.driver.session() as session:
            result = session.run(query, **params)
            return list(result)

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

def connect_to_neo4j():
    """Установка соединения с Neo4j"""
    try:
        # Параметры подключения
        uri = "neo4j://localhost:7687"
        user = "neo4j"
        password = "neo4j123"
        
        # Создание объекта для работы с Neo4j
        neo4j_demo = Neo4jDemo(uri, user, password)
        
        # Проверка соединения
        result = neo4j_demo.run_query("RETURN 1 AS result")
        if result and result[0]["result"] == 1:
            print("✅ Соединение с Neo4j установлено")
            return neo4j_demo
        else:
            print("❌ Ошибка проверки соединения с Neo4j")
            return None
    except Exception as e:
        print(f"❌ Ошибка подключения к Neo4j: {str(e)}")
        return None

def create_storage(neo4j):
    """Создание схемы данных в Neo4j (ограничения, индексы)"""
    # Создаем ограничения уникальности для основных узлов
    try:
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Group) REQUIRE g.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Student) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Course) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Lecture) REQUIRE l.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Department) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (sp:Specialty) REQUIRE sp.id IS UNIQUE"
        ]
        
        # Пробуем современный синтаксис Neo4j 4.x
        for constraint in constraints:
            try:
                neo4j.run_query(constraint)
            except Exception as e:
                # В случае ошибки, вероятно используется Neo4j 3.x
                if "SyntaxError" in str(e):
                    old_constraints = [
                        "CREATE CONSTRAINT ON (g:Group) ASSERT g.id IS UNIQUE",
                        "CREATE CONSTRAINT ON (s:Student) ASSERT s.id IS UNIQUE",
                        "CREATE CONSTRAINT ON (c:Course) ASSERT c.id IS UNIQUE",
                        "CREATE CONSTRAINT ON (l:Lecture) ASSERT l.id IS UNIQUE",
                        "CREATE CONSTRAINT ON (d:Department) ASSERT d.id IS UNIQUE",
                        "CREATE CONSTRAINT ON (sp:Specialty) ASSERT sp.id IS UNIQUE"
                    ]
                    for old_constraint in old_constraints:
                        neo4j.run_query(old_constraint)
                    break
                else:
                    raise e
        
        print("✅ Схема данных создана в Neo4j")
    except Exception as e:
        print(f"⚠️ Ошибка при создании схемы данных: {str(e)}")
        print("⚠️ Продолжаем без создания ограничений")

def add_data(neo4j, pg_cursor):
    """Добавление данных из PostgreSQL в Neo4j"""
    # Импортируем данные о кафедрах
    pg_cursor.execute("SELECT * FROM departments")
    departments = pg_cursor.fetchall()
    
    for dept in departments:
        neo4j.run_query(
            """
            CREATE (d:Department {id: $id, name: $name})
            RETURN d
            """,
            id=dept['id'],
            name=dept['name']
        )
    
    print(f"✅ Импортировано {len(departments)} кафедр в Neo4j")
    
    # Импортируем данные о специальностях
    pg_cursor.execute("SELECT * FROM specialties")
    specialties = pg_cursor.fetchall()
    
    for spec in specialties:
        neo4j.run_query(
            """
            CREATE (sp:Specialty {id: $id, name: $name, code: $code})
            RETURN sp
            """,
            id=spec['id'],
            name=spec['name'],
            code=spec['code']
        )
    
    print(f"✅ Импортировано {len(specialties)} специальностей в Neo4j")
    
    # Импортируем данные о курсах и связываем с кафедрами и специальностями
    pg_cursor.execute("SELECT c.*, d.name as dept_name, s.name as spec_name FROM courses c JOIN departments d ON c.id_kafedr_a = d.id JOIN specialties s ON c.id_spec = s.id")
    courses = pg_cursor.fetchall()
    
    for course in courses:
        neo4j.run_query(
            """
            CREATE (c:Course {id: $id, name: $name})
            WITH c
            MATCH (d:Department {id: $dept_id})
            MATCH (s:Specialty {id: $spec_id})
            CREATE (c)-[:BELONGS_TO]->(d)
            CREATE (c)-[:HAS_SPECIALTY]->(s)
            RETURN c
            """,
            id=course['id'],
            name=course['name'],
            dept_id=course['id_kafedr_a'],
            spec_id=course['id_spec']
        )
    
    print(f"✅ Импортировано {len(courses)} курсов в Neo4j")
    
    # Импортируем данные о лекциях и связываем с курсами
    pg_cursor.execute("SELECT * FROM lectures")
    lectures = pg_cursor.fetchall()
    
    for lecture in lectures:
        neo4j.run_query(
            """
            CREATE (l:Lecture {id: $id, name: $name, requirements: $requirements})
            WITH l
            MATCH (c:Course {id: $course_id})
            CREATE (l)-[:PART_OF]->(c)
            RETURN l
            """,
            id=lecture['id'],
            name=lecture['name'],
            requirements=lecture['requirements'],
            course_id=lecture['id_course']
        )
    
    print(f"✅ Импортировано {len(lectures)} лекций в Neo4j")
    
    # Импортируем данные о группах и связываем с кафедрами
    pg_cursor.execute("SELECT * FROM groups")
    groups = pg_cursor.fetchall()
    
    for group in groups:
        neo4j.run_query(
            """
            CREATE (g:Group {id: $id, name: $name, startYear: $startYear, endYear: $endYear})
            WITH g
            MATCH (d:Department {id: $dept_id})
            CREATE (g)-[:BELONGS_TO]->(d)
            RETURN g
            """,
            id=group['id'],
            name=group['name'],
            startYear=str(group['startyear']),
            endYear=str(group['endyear']),
            dept_id=group['id_kafedr_a']
        )
    
    print(f"✅ Импортировано {len(groups)} групп в Neo4j")
    
    # Импортируем данных о студентах и связываем с группами
    pg_cursor.execute("SELECT * FROM students")
    students = pg_cursor.fetchall()
    
    for student in students:
        neo4j.run_query(
            """
            CREATE (s:Student {id: $id, fio: $fio, date_of_recipient: $date_of_recipient})
            WITH s
            MATCH (g:Group {id: $group_id})
            CREATE (s)-[:MEMBER_OF]->(g)
            RETURN s
            """,
            id=student['id'],
            fio=student['fio'],
            date_of_recipient=str(student['date_of_recipient']),
            group_id=student['id_group']
        )
    
    print(f"✅ Импортировано {len(students)} студентов в Neo4j")
    
    # Создаем связи между студентами и лекциями на основе посещений
    pg_cursor.execute("""
    SELECT DISTINCT v.id_student, l.id as lecture_id 
    FROM visits v 
    JOIN schedule s ON v.id_rasp = s.id 
    JOIN lectures l ON s.id_lect = l.id
    """)
    student_lectures = pg_cursor.fetchall()
    
    for sl in student_lectures:
        neo4j.run_query(
            """
            MATCH (s:Student {id: $student_id}), (l:Lecture {id: $lecture_id})
            CREATE (s)-[:ATTENDED]->(l)
            """,
            student_id=sl['id_student'],
            lecture_id=sl['lecture_id']
        )
    
    print(f"✅ Создано {len(student_lectures)} связей между студентами и лекциями в Neo4j")
    
    # Возвращаем ID примеров для проверки
    return {
        "student_id": students[0]['id'] if students else None,
        "group_id": groups[0]['id'] if groups else None,
        "course_id": courses[0]['id'] if courses else None,
        "lecture_id": lectures[0]['id'] if lectures else None
    }

def read_sample(neo4j, ids):
    """Чтение образца данных для проверки"""
    # Получаем данные о студенте
    student_id = ids["student_id"]
    student = neo4j.run_query("MATCH (s:Student {id: $id}) RETURN s", id=student_id)
    if student:
        print(f"✅ Найден студент с ID {student_id}: {student[0]['s']['fio']}")
    else:
        print(f"❌ Студент с ID {student_id} не найден")
    
    # Получаем данные о группе студента
    result = neo4j.run_query(
        """
        MATCH (s:Student {id: $id})-[:MEMBER_OF]->(g:Group)
        RETURN g
        """, 
        id=student_id
    )
    
    if result:
        group = result[0]["g"]
        print(f"✅ Студент состоит в группе: {group['name']}")
    else:
        print(f"❌ Группа студента с ID {student_id} не найдена")
    
    # Получаем данные о лекциях студента
    result = neo4j.run_query(
        """
        MATCH (s:Student {id: $id})-[:ATTENDED]->(l:Lecture)-[:PART_OF]->(c:Course)
        RETURN l.name as lecture, c.name as course
        LIMIT 5
        """, 
        id=student_id
    )
    
    if result:
        lectures = result
        print(f"✅ Студент посетил {len(lectures)} лекций (показаны первые 5):")
        for i, lecture in enumerate(lectures):
            print(f"  {i+1}. '{lecture['lecture']}' курса '{lecture['course']}'")
    else:
        print(f"❌ Посещенные лекции студента с ID {student_id} не найдены")
    
    # Получаем общую статистику по графу
    stats = {
        "departments": neo4j.run_query("MATCH (d:Department) RETURN count(d) as count")[0]["count"],
        "specialties": neo4j.run_query("MATCH (sp:Specialty) RETURN count(sp) as count")[0]["count"],
        "courses": neo4j.run_query("MATCH (c:Course) RETURN count(c) as count")[0]["count"],
        "lectures": neo4j.run_query("MATCH (l:Lecture) RETURN count(l) as count")[0]["count"],
        "groups": neo4j.run_query("MATCH (g:Group) RETURN count(g) as count")[0]["count"],
        "students": neo4j.run_query("MATCH (s:Student) RETURN count(s) as count")[0]["count"],
        "relationships": neo4j.run_query("MATCH ()-[r]-() RETURN count(r) as count")[0]["count"]
    }
    
    print(f"\n✅ Общая статистика графа Neo4j:")
    print(f"  - Кафедр: {stats['departments']}")
    print(f"  - Специальностей: {stats['specialties']}")
    print(f"  - Курсов: {stats['courses']}")
    print(f"  - Лекций: {stats['lectures']}")
    print(f"  - Групп: {stats['groups']}")
    print(f"  - Студентов: {stats['students']}")
    print(f"  - Связей: {stats['relationships']}")
    
    # Выполняем интересный запрос: Найти топ-3 самых посещаемых лекций
    result = neo4j.run_query(
        """
        MATCH (s:Student)-[:ATTENDED]->(l:Lecture)
        WITH l, count(s) AS attendance
        RETURN l.name AS lecture, attendance
        ORDER BY attendance DESC
        LIMIT 3
        """
    )
    
    if result:
        print("\n✅ Топ-3 самых посещаемых лекций:")
        for i, lecture in enumerate(result):
            print(f"  {i+1}. '{lecture['lecture']}': {lecture['attendance']} посещений")

def main():
    """Основная функция создания и наполнения хранилища"""
    print("\n===== СОЗДАНИЕ И НАПОЛНЕНИЕ NEO4J =====")
    
    # Устанавливаем соединение с Neo4j
    neo4j = connect_to_neo4j()
    if not neo4j:
        return
    
    # Подключаемся к PostgreSQL для получения данных
    pg_connection, pg_cursor = connect_to_postgresql()
    if not pg_connection or not pg_cursor:
        return
    
    try:
        # Создаем схему данных
        create_storage(neo4j)
        
        # Импортируем данные из PostgreSQL в Neo4j
        ids = add_data(neo4j, pg_cursor)
        
        # Читаем образец для проверки
        read_sample(neo4j, ids)
        
        print("\n===== ЗАВЕРШЕНО =====")
        print("""
Для проверки данных в Neo4j:
1. Откройте Neo4j Browser по адресу: http://localhost:7474
2. Войдите с учетными данными: neo4j/neo4j123
3. Выполните запросы:
   MATCH (g:Group) RETURN g;
   MATCH (s:Student) RETURN s;
   MATCH (c:Course) RETURN c;
   MATCH (l:Lecture) RETURN l;
   MATCH (s:Student)-[:MEMBER_OF]->(g:Group) RETURN s, g LIMIT 10;
   MATCH (s:Student)-[:ATTENDED]->(l:Lecture)-[:PART_OF]->(c:Course) RETURN s, l, c LIMIT 10;

Чтобы увидеть визуализацию связей между всеми сущностями:
   MATCH p=(g:Group)<-[:MEMBER_OF]-(s:Student)-[:ATTENDED]->(l:Lecture)-[:PART_OF]->(c:Course) 
   RETURN p LIMIT 5;
        """)
    
    finally:
        # Закрываем соединения
        if neo4j:
            neo4j.close()
        if pg_cursor:
            pg_cursor.close()
        if pg_connection:
            pg_connection.close()
            print("✅ Соединения закрыты")

if __name__ == "__main__":
    main() 