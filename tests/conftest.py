"""Конфигурация pytest и общие фикстуры для тестов"""

import os
import sys
import pytest
from typing import Generator, Tuple
from pyspark.sql import SparkSession, DataFrame
from src.services.spark_service import SparkService
from src.factories.analyzer_factory import AnalyzerFactory
from src.interfaces.analyzer_interface import IProductCategoryAnalyzer

# Устанавливаем переменные окружения для PySpark ПЕРЕД созданием сессии.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Фикстура для создания Spark сессии для тестов"""
    spark = (SparkSession
             .builder
             .appName("TestProductCategoryAnalysis")
             .master("local[*]")
             .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
             .config("spark.ui.showConsoleProgress", "false")  # Отключить прогресс-бар в консоли
             .config("spark.ui.enabled", "false")              # Отключить веб-интерфейс
             .config("spark.sql.shuffle.partitions", "1")      # Для тестов на локальной машине достаточно одной партиции
             .getOrCreate())
    
    yield spark
    
    spark.stop()


@pytest.fixture
def analyzer() -> IProductCategoryAnalyzer:
    """Фикстура для создания анализатора"""
    return AnalyzerFactory.create_product_category_analyzer()


@pytest.fixture
def sample_data(spark_session: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Фикстура с тестовыми данными"""
    # Продукты
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
        (4, "Orphan Product")
    ]
    products_df = spark_session.createDataFrame(
        products_data, ["product_id", "product_name"]
    )
    
    # Категории
    categories_data = [
        (1, "Category 1"),
        (2, "Category 2"),
        (3, "Empty Category")
    ]
    categories_df = spark_session.createDataFrame(
        categories_data, ["category_id", "category_name"]
    )
    
    # Связи
    relations_data = [
        (1, 1),  # Product A -> Category 1
        (1, 2),  # Product A -> Category 2
        (2, 1),  # Product B -> Category 1
        (3, 2),  # Product C -> Category 2
        # Product 4 (Orphan Product) не имеет связей
    ]
    relations_df = spark_session.createDataFrame(
        relations_data, ["product_id", "category_id"]
    )
    
    return products_df, categories_df, relations_df