"""Тесты для моделей данных"""

import pytest
from src.models.data_models import ProductCategoryResult
from pyspark.sql import SparkSession


class TestDataModels:
    """Тесты для моделей данных"""
    
    def test_product_category_result_creation(self, spark_session: SparkSession):
        """Тест создания модели результата"""
        # Создаем тестовые DataFrame
        pairs_df = spark_session.createDataFrame(
            [("Product A", "Category 1")], ["product_name", "category_name"]
        )
        products_df = spark_session.createDataFrame(
            [("Orphan Product",)], ["product_name"]
        )
        
        result = ProductCategoryResult(
            product_category_pairs=pairs_df,
            products_without_categories=products_df,
            total_pairs=1,
            total_products_without_categories=1
        )
        
        assert result.total_pairs == 1
        assert result.total_products_without_categories == 1
        assert result.product_category_pairs is not None
        assert result.products_without_categories is not None