#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import time

def print_header(message):
    """Печать заголовка с выделением"""
    line = "=" * 60
    print(f"\n{line}")
    print(f"   {message}")
    print(f"{line}\n")

def run_script(script_name):
    """Запуск указанного скрипта"""
    print_header(f"Выполнение скрипта очистки: {script_name}")
    
    try:
        # Формируем путь к скрипту
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        
        # Запускаем скрипт с помощью Python
        result = subprocess.run([sys.executable, script_path], check=True)
        
        if result.returncode == 0:
            print(f"\n✅ Скрипт {script_name} успешно выполнен")
            return True
        else:
            print(f"\n❌ Скрипт {script_name} завершился с ошибкой (код {result.returncode})")
            return False
            
    except subprocess.SubprocessError as e:
        print(f"\n❌ Ошибка при запуске скрипта {script_name}: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {e}")
        return False

def confirm_cleanup():
    """Запрос подтверждения на удаление всех данных"""
    print_header("ВНИМАНИЕ! УДАЛЕНИЕ ДАННЫХ")
    print("""
Вы собираетесь удалить все данные из следующих хранилищ:
- Redis
- MongoDB
- Neo4j
- Elasticsearch
- PostgreSQL

Эта операция необратима и удалит все созданные данные.
""")
    confirm = input("Вы уверены, что хотите продолжить? (y/n): ")
    return confirm.lower() == 'y'

def main():
    """Основная функция для запуска всех скриптов очистки"""
    print_header("ОЧИСТКА БАЗ ДАННЫХ")
    
    if not confirm_cleanup():
        print("\n❌ Операция отменена пользователем")
        return
    
    # Список скриптов для запуска в нужном порядке
    # Порядок обратный созданию: сначала очищаем зависимые БД, затем центральную
    scripts = [
        "redis_cleanup.py",      # Сначала очищаем кэш и временные данные
        "mongodb_cleanup.py",    # Затем документную БД
        "elasticsearch_cleanup.py", # Затем поисковый индекс
        "neo4j_cleanup.py",      # Затем графовую БД
        "postgresql_cleanup.py"  # В конце центральную реляционную БД
    ]
    
    # Запускаем скрипты по очереди
    results = {}
    for script in scripts:
        results[script] = run_script(script)
        # Небольшая пауза между скриптами
        time.sleep(2)  # Увеличиваем паузу для гарантированного завершения каждого скрипта
    
    # Выводим итоговый результат
    print_header("ИТОГИ ОЧИСТКИ")
    
    success_count = 0
    for script, success in results.items():
        status = "✅ УСПЕХ" if success else "❌ ОШИБКА"
        if success:
            success_count += 1
        print(f"{status}: {script}")
    
    if all(results.values()):
        print("\n✅ Все скрипты успешно выполнены")
    else:
        print(f"\n⚠️ Выполнено успешно {success_count} из {len(scripts)} скриптов")
        
    print("""
Все хранилища данных были очищены в следующем порядке:

1. Redis - кэш и временные данные о студентах
2. MongoDB - документы групп с вложенными студентами
3. ElasticSearch - индексы материалов для полнотекстового поиска
4. Neo4j - графовая структура связей между сущностями
5. PostgreSQL - центральная реляционная БД с основными таблицами

Теперь вы можете запустить скрипт create_all.py для повторного создания и заполнения всех хранилищ данных.
""")

if __name__ == "__main__":
    main() 