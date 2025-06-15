"""Модуль с интерфейсами для анализаторов"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from src.models.data_models import ProductCategoryResult


class IProductCategoryAnalyzer(ABC):
    """Интерфейс для анализатора продуктов и категорий."""

    def __init__(self):
        self.validator = None

    @abstractmethod
    def analyze_product_categories(
        self,
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> ProductCategoryResult:
        """
        Анализирует связи между продуктами и категориями.
        Args:
            products_df: DataFrame с продуктами
            categories_df: DataFrame с категориями
            product_category_relations_df: DataFrame со связями продукт-категория
            
        Returns:
            Результат анализа
        """
        pass

    @abstractmethod
    def get_unified_result(
            self,
            products_df: DataFrame,
            categories_df: DataFrame,
            product_category_relations_df: DataFrame
    ) -> DataFrame:
        """
        Возвращает единый DataFrame со всеми парами продукт-категория
        и продуктами без категорий.

        Args:
            products_df: DataFrame с продуктами
            categories_df: DataFrame с категориями
            product_category_relations_df: DataFrame со связями

        Returns:
            Объединенный DataFrame с колонками: product_name, category_name
        """
        pass