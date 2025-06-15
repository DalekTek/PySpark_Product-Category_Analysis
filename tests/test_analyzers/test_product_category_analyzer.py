"""Тесты для анализатора продуктов и категорий"""

import pytest
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from src.exceptions.custom_exceptions import DataFrameValidationError
from src.interfaces.analyzer_interface import IProductCategoryAnalyzer


class TestProductCategoryAnalyzer:
    def test_analyze_product_categories_success(
            self, 
            analyzer: IProductCategoryAnalyzer, 
            sample_data: Tuple[DataFrame, DataFrame, DataFrame]
    ):
        """Тест успешного анализа продуктов и категорий"""
        products_df, categories_df, relations_df = sample_data
        
        result = analyzer.analyze_product_categories(
            products_df, categories_df, relations_df
        )
        
        # Проверяем, что результат содержит ожидаемое количество записей
        assert result.total_pairs == 4  # Product A (2), Product B (1), Product C (1)
        assert result.total_products_without_categories == 1  # Orphan Product
        
        # Проверяем структуру результата
        assert isinstance(result.product_category_pairs, DataFrame)
        assert isinstance(result.products_without_categories, DataFrame)
    
    def test_get_unified_result(
        self,
        analyzer: IProductCategoryAnalyzer,
        sample_data: Tuple[DataFrame, DataFrame, DataFrame]
    ):
        """Тест получения объединенного результата"""
        products_df, categories_df, relations_df = sample_data
        
        unified_result = analyzer.get_unified_result(
            products_df, categories_df, relations_df
        )
        
        # Проверяем структуру
        expected_columns = {"product_name", "category_name"}
        assert set(unified_result.columns) == expected_columns
        
        # Проверяем количество записей (4 пары + 1 продукт без категории)
        assert unified_result.count() == 5
    
    def test_validation_missing_columns(
        self,
        analyzer: IProductCategoryAnalyzer,
        spark_session: SparkSession
    ):
        """Тест валидации при отсутствии обязательных колонок"""
        # Создаем DataFrame с неправильными колонками
        invalid_products = spark_session.createDataFrame(
            [(1, "Product")], ["wrong_id", "wrong_name"]
        )
        categories_df = spark_session.createDataFrame(
            [(1, "Category")], ["category_id", "category_name"]
        )
        relations_df = spark_session.createDataFrame(
            [(1, 1)], ["product_id", "category_id"]
        )
        
        with pytest.raises(DataFrameValidationError):
            analyzer.analyze_product_categories(
                invalid_products, categories_df, relations_df
            )
    
    def test_empty_relations(
        self,
        analyzer: IProductCategoryAnalyzer,
        spark_session: SparkSession
    ):
        """Тест работы с пустыми связями"""
        products_df = spark_session.createDataFrame(
            [(1, "Product A")], ["product_id", "product_name"]
        )
        categories_df = spark_session.createDataFrame(
            [(1, "Category 1")], ["category_id", "category_name"]
        )

        # Определяем схему явно
        relations_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ])

        # Создаем пустой DataFrame с явно заданной схемой
        relations_df = spark_session.createDataFrame(
            [], schema=relations_schema
        )
        
        result = analyzer.analyze_product_categories(
            products_df, categories_df, relations_df
        )
        
        # Все продукты должны быть без категорий
        assert result.total_pairs == 0
        assert result.total_products_without_categories == 1