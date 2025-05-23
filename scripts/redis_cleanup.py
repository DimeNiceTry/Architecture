#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import json

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

def check_data(r):
    """Проверка наличия данных студентов в Redis"""
    # Проверяем наличие ключа-метки
    if not r.exists("students:info"):
        print("❌ В Redis нет данных студентов для удаления")
        return False
    
    # Проверяем наличие ID студентов
    student_ids = r.smembers("students:all")
    if not student_ids:
        print("❌ В Redis нет ID студентов для удаления")
        return False
    
    print(f"✅ Найдено {len(student_ids)} записей студентов для удаления")
    return True

def delete_storage(r):
    """Удаление всего хранилища данных"""
    # Получаем список всех студентов
    student_ids = r.smembers("students:all")
    
    # Удаляем данные каждого студента
    deleted_count = 0
    for student_id in student_ids:
        r.delete(f"student:{student_id}")
        deleted_count += 1
    
    print(f"✅ Удалено {deleted_count} записей о студентах")
    
    # Удаляем служебные ключи
    r.delete("students:all")
    r.delete("students:info")
    
    print("✅ Хранилище данных студентов удалено")
    
    # Проверяем, что все удалено
    remaining_keys = r.keys("student:*")
    if remaining_keys:
        print(f"⚠️ Внимание: остались {len(remaining_keys)} ключей student:*")
    else:
        print("✅ Все ключи student:* успешно удалены")

def main():
    """Основная функция удаления хранилища"""
    print("\n===== УДАЛЕНИЕ ДАННЫХ ИЗ REDIS =====")
    
    # Устанавливаем соединение с Redis
    r = connect_to_redis()
    if not r:
        return
    
    # Проверяем наличие данных
    if not check_data(r):
        return
    
    # Запрашиваем подтверждение
    confirm = input("Вы уверены, что хотите удалить все данные о студентах из Redis? (y/n): ")
    if confirm.lower() != 'y':
        print("❌ Операция удаления отменена.")
        return
    
    # Удаляем данные
    delete_storage(r)
    
    print("\n===== УДАЛЕНИЕ ЗАВЕРШЕНО =====")

if __name__ == "__main__":
    main() 