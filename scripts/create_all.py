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
    print_header(f"Выполнение скрипта: {script_name}")
    
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

def main():
    """Основная функция для запуска всех скриптов создания данных"""
    print_header("СОЗДАНИЕ И ЗАПОЛНЕНИЕ БАЗ ДАННЫХ")
    
    # Список скриптов для запуска в нужном порядке
    # Сначала создаем PostgreSQL как центральную БД, затем остальные, которые берут из неё данные
    scripts = [
        "postgresql_create.py",  # Первым запускаем PostgreSQL, так как он является центральной БД
        "neo4j_create.py",       # Затем остальные БД, которые используют данные из PostgreSQL
        "elasticsearch_create.py",
        "mongodb_create.py",
        "redis_create.py"
    ]
    
    # Запускаем скрипты по очереди
    results = {}
    for script in scripts:
        results[script] = run_script(script)
        # Небольшая пауза между скриптами
        time.sleep(2)  # Увеличиваем паузу для гарантированного завершения каждого скрипта
    
    # Выводим итоговый результат
    print_header("ИТОГИ ВЫПОЛНЕНИЯ")
    
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
Данные во всех хранилищах были созданы и заполнены согласно архитектурной схеме:

1. PostgreSQL - центральная реляционная БД с основными таблицами системы
2. Neo4j - графовая БД для хранения связей между сущностями
3. ElasticSearch - полнотекстовый поиск по материалам курсов
4. MongoDB - документо-ориентированная БД для хранения групп с вложенными студентами
5. Redis - ключ-значение хранилище для быстрого доступа к данным студентов и кэширования

Для проверки данных вы можете использовать соответствующие клиенты:

1. PostgreSQL: psql -U postgres -h localhost
2. Neo4j: http://localhost:7474 (Neo4j Browser)
3. ElasticSearch: http://localhost:9200/_cat/indices?v или Kibana
4. MongoDB: mongosh или MongoDB Compass
5. Redis: redis-cli

Для удаления данных запустите скрипт cleanup_all.py
""")

if __name__ == "__main__":
    main() 