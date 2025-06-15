.PHONY: install test lint format clean run

# Установка зависимостей
install:
	pip install -e .[dev]

# Запуск тестов
test:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Запуск тестов с покрытием
test-coverage:
	pytest tests/ --cov=src --cov-report=html --cov-report=term --cov-fail-under=80

# Линтинг кода
lint:
	flake8 src/ tests/
	mypy src/

# Форматирование кода
format:
	black src/ tests/
	isort src/ tests/

# Очистка временных файлов
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

# Запуск основного приложения
run:
	python main.py

# Запуск интеграционных тестов
test-integration:
	pytest tests/ -m integration -v

# Проверка качества кода
quality-check: lint test

# Полная сборка проекта
build: clean format quality-check
	python setup.py sdist bdist_wheel