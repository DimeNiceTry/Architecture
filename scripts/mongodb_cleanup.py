#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo
import json

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

def check_data(client):
    """Проверка наличия данных в MongoDB"""
    # Проверяем наличие базы данных
    if 'university' not in client.list_database_names():
        print("❌ База данных 'university' не существует")
        return False
    
    # Проверяем наличие коллекций
    db = client['university']
    if 'groups' not in db.list_collection_names():
        print("❌ Коллекция 'groups' не существует")
        return False
    
    # Проверяем наличие данных
    count = db.groups.count_documents({})
    print(f"✅ Найдено {count} групп(а) для удаления")
    
    return count > 0

def show_data_summary(client):
    """Показывает сводку данных перед удалением"""
    db = client['university']
    groups = db.groups.find()
    
    print("\nСводка данных для удаления:")
    for group in groups:
        print(f"- Группа: {group['name']}, Год начала: {group['startYear']}, Студентов: {len(group.get('students', []))}")

def delete_collection(client):
    """Удаление коллекции groups"""
    db = client['university']
    db.groups.drop()
    print("✅ Коллекция 'groups' удалена")

def delete_database(client):
    """Удаление всей базы данных"""
    client.drop_database('university')
    print("✅ База данных 'university' удалена")

def main():
    """Основная функция удаления хранилища"""
    print("\n===== УДАЛЕНИЕ ДАННЫХ ИЗ MONGODB =====")
    
    # Устанавливаем соединение с MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    # Проверяем наличие данных
    if not check_data(client):
        return
    
    # Показываем сводку данных
    show_data_summary(client)
    
    # Спрашиваем, что именно удалять
    print("\nВыберите действие:")
    print("1. Удалить только коллекцию 'groups'")
    print("2. Удалить всю базу данных 'university'")
    print("3. Отмена")
    
    choice = input("Введите номер действия (1-3): ")
    
    if choice == '1':
        confirm = input("Вы точно хотите удалить коллекцию 'groups'? (y/n): ")
        if confirm.lower() == 'y':
            delete_collection(client)
        else:
            print("❌ Операция удаления отменена.")
    elif choice == '2':
        confirm = input("Вы точно хотите удалить всю базу данных 'university'? (y/n): ")
        if confirm.lower() == 'y':
            delete_database(client)
        else:
            print("❌ Операция удаления отменена.")
    else:
        print("❌ Операция удаления отменена.")
    
    print("\n===== УДАЛЕНИЕ ЗАВЕРШЕНО =====")

if __name__ == "__main__":
    main() 