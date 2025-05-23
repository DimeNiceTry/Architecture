#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j import GraphDatabase
import json

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

def check_data(neo4j):
    """Проверка наличия данных в Neo4j"""
    # Проверяем наличие узлов и связей
    stats = {
        "groups": neo4j.run_query("MATCH (g:Group) RETURN count(g) as count")[0]["count"],
        "students": neo4j.run_query("MATCH (s:Student) RETURN count(s) as count")[0]["count"],
        "courses": neo4j.run_query("MATCH (c:Course) RETURN count(c) as count")[0]["count"],
        "relationships": neo4j.run_query("MATCH ()-[r]-() RETURN count(r) as count")[0]["count"]
    }
    
    if all(value == 0 for value in stats.values()):
        print("❌ В Neo4j нет данных для удаления")
        return False
    
    print(f"\n✅ Данные в Neo4j для удаления:")
    print(f"  - Групп: {stats['groups']}")
    print(f"  - Студентов: {stats['students']}")
    print(f"  - Курсов: {stats['courses']}")
    print(f"  - Связей: {stats['relationships']}")
    
    return True

def show_data_sample(neo4j):
    """Показывает образец данных перед удалением"""
    # Пример групп
    groups = neo4j.run_query("MATCH (g:Group) RETURN g.name, g.id LIMIT 3")
    if groups:
        print("\nГруппы:")
        for group in groups:
            print(f"  - {group['g.name']} (ID: {group['g.id']})")
    
    # Пример студентов
    students = neo4j.run_query("MATCH (s:Student) RETURN s.fio, s.id LIMIT 3")
    if students:
        print("\nСтуденты:")
        for student in students:
            print(f"  - {student['s.fio']} (ID: {student['s.id']})")
    
    # Пример курсов
    courses = neo4j.run_query("MATCH (c:Course) RETURN c.name, c.id, c.required LIMIT 3")
    if courses:
        print("\nКурсы:")
        for course in courses:
            course_type = "обязательный" if course['c.required'] else "по выбору"
            print(f"  - {course['c.name']} (ID: {course['c.id']}, тип: {course_type})")

def delete_all_data(neo4j):
    """Удаление всех данных из Neo4j"""
    # Удаляем связи
    rel_count = neo4j.run_query("MATCH ()-[r]-() DELETE r RETURN count(r) as count")[0]["count"] if "count" in neo4j.run_query("MATCH ()-[r]-() DELETE r RETURN count(r) as count")[0] else 0
    print(f"✅ Удалено связей: {rel_count}")
    
    # Удаляем узлы
    nodes_count = neo4j.run_query("MATCH (n) DELETE n RETURN count(n) as count")[0]["count"] if "count" in neo4j.run_query("MATCH (n) DELETE n RETURN count(n) as count")[0] else 0
    print(f"✅ Удалено узлов: {nodes_count}")
    
    return rel_count, nodes_count

def delete_constraints(neo4j):
    """Удаление ограничений из Neo4j"""
    try:
        # Получаем все ограничения
        constraints = neo4j.run_query("CALL db.constraints()")
        
        if not constraints:
            print("✅ В базе нет ограничений для удаления")
            return 0
        
        deleted_count = 0
        
        # Удаляем каждое ограничение по его имени или описанию
        for constraint in constraints:
            try:
                if "name" in constraint:
                    # Neo4j 4.x
                    neo4j.run_query(f"DROP CONSTRAINT {constraint['name']}")
                    deleted_count += 1
                else:
                    # Neo4j 3.x - используем описание
                    constraint_desc = constraint.get("description", "")
                    if "ON (g:Group) ASSERT g.id IS UNIQUE" in constraint_desc:
                        neo4j.run_query("DROP CONSTRAINT ON (g:Group) ASSERT g.id IS UNIQUE")
                        deleted_count += 1
                    elif "ON (s:Student) ASSERT s.id IS UNIQUE" in constraint_desc:
                        neo4j.run_query("DROP CONSTRAINT ON (s:Student) ASSERT s.id IS UNIQUE")
                        deleted_count += 1
                    elif "ON (c:Course) ASSERT c.id IS UNIQUE" in constraint_desc:
                        neo4j.run_query("DROP CONSTRAINT ON (c:Course) ASSERT c.id IS UNIQUE")
                        deleted_count += 1
            except Exception as e:
                print(f"⚠️ Ошибка при удалении ограничения: {str(e)}")
        
        print(f"✅ Удалено {deleted_count} ограничений")
        return deleted_count
    
    except Exception as e:
        print(f"⚠️ Ошибка при удалении ограничений: {str(e)}")
        return 0

def main():
    """Основная функция удаления хранилища"""
    print("\n===== УДАЛЕНИЕ ДАННЫХ ИЗ NEO4J =====")
    
    # Устанавливаем соединение с Neo4j
    neo4j = connect_to_neo4j()
    if not neo4j:
        return
    
    try:
        # Проверяем наличие данных
        if not check_data(neo4j):
            return
        
        # Показываем пример данных
        show_data_sample(neo4j)
        
        # Запрашиваем подтверждение
        print("\nВыберите действие:")
        print("1. Удалить только данные (узлы и связи)")
        print("2. Удалить данные и ограничения")
        print("3. Отмена")
        
        choice = input("Введите номер действия (1-3): ")
        
        if choice == '1':
            confirm = input("Вы точно хотите удалить все данные из Neo4j? (y/n): ")
            if confirm.lower() == 'y':
                delete_all_data(neo4j)
            else:
                print("❌ Операция удаления отменена.")
        elif choice == '2':
            confirm = input("Вы точно хотите удалить все данные и ограничения из Neo4j? (y/n): ")
            if confirm.lower() == 'y':
                delete_all_data(neo4j)
                delete_constraints(neo4j)
            else:
                print("❌ Операция удаления отменена.")
        else:
            print("❌ Операция удаления отменена.")
    
    finally:
        # Закрываем соединение
        if neo4j:
            neo4j.close()
    
    print("\n===== УДАЛЕНИЕ ЗАВЕРШЕНО =====")

if __name__ == "__main__":
    main() 