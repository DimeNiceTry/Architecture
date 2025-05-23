#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import importlib
import os

def run_demo(module_name, title):
    """Запуск демонстрации из указанного модуля"""
    print(f"\n{'=' * 80}")
    print(f"ЗАПУСК ДЕМОНСТРАЦИИ: {title}")
    print(f"{'=' * 80}")
    
    try:
        module = importlib.import_module(module_name)
        module.main()
    except Exception as e:
        print(f"❌ Ошибка при запуске демонстрации {module_name}: {str(e)}")
    
    print(f"{'=' * 80}")
    print(f"ЗАВЕРШЕНИЕ ДЕМОНСТРАЦИИ: {title}")
    print(f"{'=' * 80}\n")

def main():
    """Основная функция для запуска всех демонстраций"""
    print("\n")
    print("*" * 100)
    print("*" + " " * 35 + "ДЕМОНСТРАЦИЯ РАБОТЫ С ХРАНИЛИЩАМИ" + " " * 35 + "*")
    print("*" * 100)
    print("\n")
    
    demos = [
        ("redis_operations", "Redis - Хранение списка студентов (ключ-значение)"),
        ("mongodb_operations", "MongoDB - Документ с данными и составом группы"),
        ("neo4j_operations", "Neo4j - Связи между группой-студентом-курсом"),
        ("elasticsearch_operations", "ElasticSearch - Полнотекстовые описания курсов"),
        ("postgresql_operations", "PostgreSQL - Данные о посещении с партиционированием")
    ]
    
    for i, (module_name, title) in enumerate(demos):
        run_demo(module_name, title)
        
        # Делаем паузу между демонстрациями, кроме последней
        if i < len(demos) - 1:
            print("Пауза перед следующей демонстрацией (3 секунды)...")
            time.sleep(3)
    
    print("\n")
    print("*" * 100)
    print("*" + " " * 38 + "ВСЕ ДЕМОНСТРАЦИИ ЗАВЕРШЕНЫ" + " " * 38 + "*")
    print("*" * 100)
    print("\n")

if __name__ == "__main__":
    main() 