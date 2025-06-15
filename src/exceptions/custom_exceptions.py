"""Модуль пользовательских исключений для проекта"""


class ProductCategoryAnalysisError(Exception):
    """Базовое исключение для ошибок анализа продуктов и категорий."""
    pass


class DataFrameValidationError(ProductCategoryAnalysisError):
    """Исключение для ошибок валидации DataFrame."""
    pass


class SparkSessionError(ProductCategoryAnalysisError):
    """Исключение для ошибок работы с Spark сессией."""
    pass