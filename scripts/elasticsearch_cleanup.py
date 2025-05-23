#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
import json

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

def check_data(es):
    """Проверка наличия данных в Elasticsearch"""
    try:
        # Проверяем существует ли индекс
        if not es.indices.exists(index="courses"):
            print("❌ Индекс 'courses' не существует")
            return False
        
        # Получаем количество документов
        try:
            # ES 7.x
            count_result = es.count(index="courses")
            count = count_result.get("count", 0)
        except TypeError:
            # ES 8.x
            count = es.count(index="courses").get("count", 0) if hasattr(es.count(index="courses"), "get") else 0
        
        if count == 0:
            print("❌ В индексе 'courses' нет документов")
            return False
        
        print(f"✅ В индексе 'courses' найдено {count} документов")
        return True
    except Exception as e:
        print(f"❌ Ошибка при проверке данных: {str(e)}")
        return False

def show_data_sample(es):
    """Показывает образец данных перед удалением"""
    try:
        # Выполняем поиск по всем документам с лимитом
        try:
            # ES 7.x
            result = es.search(
                index="courses",
                body={"query": {"match_all": {}}, "size": 3}
            )
        except TypeError:
            # ES 8.x+
            result = es.search(
                index="courses",
                query={"match_all": {}},
                size=3
            )
        
        hits = result.get("hits", {}).get("hits", [])
        
        if not hits:
            print("❌ Не удалось получить образцы данных")
            return
        
        print("\nПримеры курсов для удаления:")
        for hit in hits:
            source = hit.get("_source", {})
            print(f"  - {source.get('name', 'Неизвестный курс')} (ID: {source.get('id', hit.get('_id', 'Неизвестно'))})")
            print(f"    Код: {source.get('code', 'Н/Д')}, Кафедра: {source.get('department', 'Н/Д')}")
            print(f"    Специализация: {source.get('specialization', 'Н/Д')}")
            print()
            
    except Exception as e:
        print(f"❌ Ошибка при получении образцов данных: {str(e)}")

def delete_all_documents(es):
    """Удаление всех документов из индекса"""
    try:
        # ES 7.x & 8.x
        try:
            result = es.delete_by_query(
                index="courses",
                body={"query": {"match_all": {}}}
            )
            deleted = result.get("deleted", 0)
        except TypeError:
            # ES 8.x+
            result = es.delete_by_query(
                index="courses",
                query={"match_all": {}}
            )
            deleted = result.get("deleted", 0)
        
        print(f"✅ Удалено {deleted} документов из индекса 'courses'")
        return deleted
    except Exception as e:
        print(f"❌ Ошибка при удалении документов: {str(e)}")
        
        # Пробуем альтернативный метод - удалить индекс и пересоздать его
        try:
            print("⚠️ Пробуем альтернативный метод удаления...")
            es.indices.delete(index="courses")
            print("✅ Индекс 'courses' удален")
            es.indices.create(index="courses")
            print("✅ Индекс 'courses' пересоздан (пустой)")
            return True
        except Exception as e2:
            print(f"❌ Критическая ошибка удаления: {str(e2)}")
            return False

def delete_index(es):
    """Удаление всего индекса"""
    try:
        if es.indices.exists(index="courses"):
            es.indices.delete(index="courses")
            print("✅ Индекс 'courses' полностью удален")
            return True
        else:
            print("⚠️ Индекс 'courses' не существует")
            return False
    except Exception as e:
        print(f"❌ Ошибка при удалении индекса: {str(e)}")
        return False

def main():
    """Основная функция удаления хранилища"""
    print("\n===== УДАЛЕНИЕ ДАННЫХ ИЗ ELASTICSEARCH =====")
    
    # Устанавливаем соединение с Elasticsearch
    es = connect_to_elasticsearch()
    if not es:
        return
    
    # Проверяем наличие данных
    if not check_data(es):
        print("\nВыберите действие:")
        print("1. Все равно удалить индекс 'courses' (если он существует)")
        print("2. Отмена")
        
        choice = input("Введите номер действия (1-2): ")
        
        if choice == '1':
            delete_index(es)
        else:
            print("❌ Операция удаления отменена.")
        return
    
    # Показываем сводку данных
    show_data_sample(es)
    
    # Спрашиваем, что именно удалять
    print("\nВыберите действие:")
    print("1. Удалить только документы (очистить индекс)")
    print("2. Удалить индекс полностью")
    print("3. Отмена")
    
    choice = input("Введите номер действия (1-3): ")
    
    if choice == '1':
        confirm = input("Вы точно хотите удалить все документы из индекса 'courses'? (y/n): ")
        if confirm.lower() == 'y':
            delete_all_documents(es)
        else:
            print("❌ Операция удаления отменена.")
    elif choice == '2':
        confirm = input("Вы точно хотите удалить весь индекс 'courses'? (y/n): ")
        if confirm.lower() == 'y':
            delete_index(es)
        else:
            print("❌ Операция удаления отменена.")
    else:
        print("❌ Операция удаления отменена.")
    
    print("\n===== УДАЛЕНИЕ ЗАВЕРШЕНО =====")

if __name__ == "__main__":
    main() 