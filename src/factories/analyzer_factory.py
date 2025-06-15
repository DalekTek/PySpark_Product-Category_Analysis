"""Модуль фабрики для создания анализаторов"""

from src.analyzers.product_category_analyzer import ProductCategoryAnalyzer
from src.validators.dataframe_validator import DataFrameValidator
from src.interfaces.analyzer_interface import IProductCategoryAnalyzer


class AnalyzerFactory:
    """Фабрика для создания анализаторов"""
    @staticmethod
    def create_product_category_analyzer() -> IProductCategoryAnalyzer:
        """
        Создает анализатор продуктов и категорий.
        Returns:
            Экземпляр анализатора продуктов и категорий
        """
        validator = DataFrameValidator()
        return ProductCategoryAnalyzer(validator)