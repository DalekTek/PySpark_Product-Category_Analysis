"""Модуль с моделями данных для проекта"""

from dataclasses import dataclass
from typing import Optional, List
from pyspark.sql import DataFrame


@dataclass
class ProductCategoryResult:
    """
    Модель результата анализа продуктов и категорий.
    
    Attributes:
        product_category_pairs: DataFrame с парами продукт-категория
        products_without_categories: DataFrame с продуктами без категорий
        total_pairs: Общее количество пар
        total_products_without_categories: Количество продуктов без категорий
    """
    product_category_pairs: DataFrame
    products_without_categories: DataFrame
    total_pairs: int
    total_products_without_categories: int