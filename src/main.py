"""Главный модуль приложения для демонстрации работы анализатора"""

import os
import sys
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.services.spark_service import SparkService
from src.factories.analyzer_factory import AnalyzerFactory
from src.config.logging_config import LoggerConfig

# Указываем Spark, какой Python использовать (для Windows)
# sys.executable - это путь к интерпретатору Python, который выполняет скрипт.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def create_sample_data(spark_service: SparkService) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Создает тестовые данные для демонстрации.
    Args:
        spark_service: Сервис для работы с Spark
    Returns:
        Кортеж с DataFrame продуктов, категорий и связей
    """
    spark = spark_service.get_spark_session()

    # --- Схема для продуктов ---
    products_schema = StructType([
        StructField("product_id", IntegerType(), False), # ID не может быть null
        StructField("product_name", StringType(), True)
    ])

    # Создаем тестовые продукты
    products_data = [
        (1, "iPhone 14"),
        (2, "Samsung Galaxy S23"),
        (3, "MacBook Pro"),
        (4, "Dell XPS 13"),
        (5, "AirPods Pro"),
        (6, "Orphan Product")  # Продукт без категории
    ]
    
    products_df = spark.createDataFrame(
        products_data, 
        schema=products_schema
    )

    # --- Схема для категорий ---
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), True)
    ])

    # Создаем тестовые категории
    categories_data = [
        (1, "Smartphones"),
        (2, "Laptops"),
        (3, "Audio"),
        (4, "Tablets"),
        (5, "Empty Category")  # Категория без продуктов
    ]
    
    categories_df = spark.createDataFrame(
        categories_data,
        schema=categories_schema
    )

    # --- Схема для связей ---
    relations_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])

    # Создаем связи продукт-категория
    relations_data = [
        (1, 1),  # iPhone 14 -> Smartphones
        (2, 1),  # Samsung Galaxy S23 -> Smartphones
        (3, 2),  # MacBook Pro -> Laptops
        (3, 3),  # MacBook Pro -> Audio (для демонстрации множественных категорий)
        (4, 2),  # Dell XPS 13 -> Laptops
        (5, 3),  # AirPods Pro -> Audio
        # Продукт 6 (Orphan Product) намеренно не включен
    ]
    
    relations_df = spark.createDataFrame(
        relations_data,
        schema=relations_schema
    )
    
    return products_df, categories_df, relations_df


def main() -> None:
    """Главная функция приложения."""
    logger = LoggerConfig.setup_logger("Main")
    logger.info("Starting Product-Category Analysis application")
    # Инициализация сервисов
    spark_service = SparkService()
    try:
        # Вывод информации о версии Spark и Hadoop
        spark_service.show_spark_version()

        analyzer = AnalyzerFactory.create_product_category_analyzer()
        
        # Создание тестовых данных
        logger.info("Creating sample data")
        products_df, categories_df, relations_df = create_sample_data(spark_service)
        
        # Выполнение анализа
        logger.info("Performing analysis")
        result = analyzer.analyze_product_categories(
            products_df, categories_df, relations_df
        )
        
        # Вывод результатов
        logger.info("Displaying results")
        print("\n=== Product-Category Pairs ===")
        result.product_category_pairs.show(truncate=False)
        
        print("\n=== Products Without Categories ===")
        result.products_without_categories.show(truncate=False)
        
        print(f"\nStatistics:")
        print(f"Total product-category pairs: {result.total_pairs}")
        print(f"Products without categories: {result.total_products_without_categories}")
        
        # Демонстрация объединенного результата
        print("\n=== Unified Result ===")
        unified_result = analyzer.get_unified_result(
            products_df, categories_df, relations_df
        )
        unified_result.show(truncate=False)
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed with error: {str(e)}")
        raise
    finally:
        # Очистка ресурсов
        spark_service.stop_spark_session()
        logger.info("Application finished")


if __name__ == "__main__":
    main()