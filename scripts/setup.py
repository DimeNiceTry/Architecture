#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import subprocess
import os
import time
import platform

def print_banner(text):
    """Вывод баннера"""
    width = 80
    print("\n" + "=" * width)
    print(f"{text.center(width)}")
    print("=" * width + "\n")

def check_docker():
    """Проверка доступности Docker"""
    print("Проверка Docker...")
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker установлен: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker не установлен или не доступен")
            return False
    except FileNotFoundError:
        print("❌ Docker не установлен или не в PATH")
        return False

def check_python_version():
    """Проверка версии Python"""
    print("Проверка версии Python...")
    major, minor = sys.version_info[:2]
    if major == 3 and minor >= 6:
        print(f"✅ Python {major}.{minor} совместим")
        return True
    else:
        print(f"❌ Python {major}.{minor} не совместим. Требуется Python 3.6 или выше")
        return False

def install_dependencies():
    """Установка зависимостей Python"""
    print("Установка зависимостей Python...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], check=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("✅ Зависимости успешно установлены")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при установке зависимостей: {e}")
        # Пробуем установку с флагом --user
        try:
            print("Пробуем установку с флагом --user...")
            subprocess.run([sys.executable, "-m", "pip", "install", "--user", "-r", "requirements.txt"], check=True)
            print("✅ Зависимости успешно установлены с флагом --user")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Не удалось установить зависимости: {e}")
            return False

def check_containers():
    """Проверка запущенных контейнеров"""
    print("Проверка запущенных контейнеров...")
    try:
        result = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
        if result.returncode == 0:
            containers = result.stdout.strip().split('\n')
            required_containers = ["redis", "mongo", "neo4j", "elasticsearch", "postgres"]
            running_required = [c for c in required_containers if c in containers]
            
            if running_required:
                print(f"✅ Запущены контейнеры: {', '.join(running_required)}")
            
            missing = [c for c in required_containers if c not in containers]
            if missing:
                print(f"❌ Не запущены контейнеры: {', '.join(missing)}")
                return False
            return True
        else:
            print("❌ Не удалось получить список контейнеров")
            return False
    except FileNotFoundError:
        print("❌ Docker не установлен или не в PATH")
        return False

def start_containers():
    """Запуск контейнеров через docker-compose"""
    print("Запуск контейнеров...")
    try:
        # Перейти к корневой директории проекта
        os.chdir('..')
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("✅ Контейнеры запущены. Ожидание инициализации...")
        # Даем время на инициализацию
        time.sleep(10)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при запуске контейнеров: {e}")
        return False
    except FileNotFoundError:
        print("❌ Docker Compose не установлен или не в PATH")
        return False

def main():
    """Основная функция настройки окружения"""
    print_banner("НАСТРОЙКА ОКРУЖЕНИЯ ДЛЯ ДЕМОНСТРАЦИИ ХРАНИЛИЩ ДАННЫХ")
    
    # Проверяем окружение
    python_ok = check_python_version()
    if not python_ok:
        print("❌ Необходимо обновить Python до версии 3.6 или выше")
        sys.exit(1)
    
    # Устанавливаем зависимости
    deps_ok = install_dependencies()
    if not deps_ok:
        print("⚠️ Проблемы с установкой зависимостей")
    
    # Проверяем Docker
    docker_ok = check_docker()
    if not docker_ok:
        print("❌ Для запуска демонстрации необходим Docker")
        sys.exit(1)
    
    # Проверяем/запускаем контейнеры
    containers_ok = check_containers()
    if not containers_ok:
        print("Контейнеры не запущены. Запускаем...")
        if not start_containers():
            print("❌ Не удалось запустить контейнеры. Проверьте docker-compose.yml")
            sys.exit(1)
    
    print_banner("ОКРУЖЕНИЕ НАСТРОЕНО")
    print("\nДля запуска демонстрации выполните:\n")
    if platform.system() == 'Windows':
        print("  python run_all.py")
    else:
        print("  python3 run_all.py")
    print("\nДля запуска отдельных демонстраций:\n")
    if platform.system() == 'Windows':
        print("  python redis_operations.py")
        print("  python mongodb_operations.py")
        print("  python neo4j_operations.py")
        print("  python elasticsearch_operations.py")
        print("  python postgresql_operations.py")
    else:
        print("  python3 redis_operations.py")
        print("  python3 mongodb_operations.py")
        print("  python3 neo4j_operations.py")
        print("  python3 elasticsearch_operations.py")
        print("  python3 postgresql_operations.py")

if __name__ == "__main__":
    main() 