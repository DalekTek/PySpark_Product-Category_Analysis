"""Модуль для анализа продуктов и категорий"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.interfaces.analyzer_interface import IProductCategoryAnalyzer
from src.models.data_models import ProductCategoryResult
from src.validators.dataframe_validator import DataFrameValidator
from src.config.logging_config import LoggerConfig


class ProductCategoryAnalyzer(IProductCategoryAnalyzer):
    """
    Анализатор для обработки связей между продуктами и категориями.
    Класс реализует бизнес-логику для:
    - Получения всех пар продукт-категория
    - Поиска продуктов без категорий
    - Объединения результатов в один DataFrame
    """
    
    def __init__(self, validator: Optional[DataFrameValidator] = None):
        self.validator = validator or DataFrameValidator()
        self.logger = LoggerConfig.setup_logger(self.__class__.__name__)
    
    def analyze_product_categories(
        self,
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> ProductCategoryResult:
        """
        Анализирует связи между продуктами и категориями.
        Args:
            products_df: DataFrame с продуктами (колонки: product_id, product_name)
            categories_df: DataFrame с категориями (колонки: category_id, category_name)
            product_category_relations_df: DataFrame со связями (колонки: product_id, category_id)
        Returns:
            Объединенный результат анализа
        """
        self.logger.info("Starting product-category analysis")
        
        # Валидация входных данных
        self._validate_input_dataframes(
            products_df, categories_df, product_category_relations_df
        )
        
        # Получение пар продукт-категория
        product_category_pairs = self._get_product_category_pairs(
            products_df, categories_df, product_category_relations_df
        )
        
        # Получение продуктов без категорий
        products_without_categories = self._get_products_without_categories(
            products_df, product_category_relations_df
        )
        
        # Подсчет статистики
        pairs_count = product_category_pairs.count()
        no_category_count = products_without_categories.count()
        
        self.logger.info(
            f"Analysis completed. Product-category pairs: {pairs_count}, "
            f"Products without categories: {no_category_count}"
        )
        
        return ProductCategoryResult(
            product_category_pairs=product_category_pairs,
            products_without_categories=products_without_categories,
            total_pairs=pairs_count,
            total_products_without_categories=no_category_count
        )
    
    def get_unified_result(
        self,
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> DataFrame:
        """
        Возвращает единый DataFrame со всеми парами продукт-категория и продуктами без категорий.
        Args:
            products_df: DataFrame с продуктами
            categories_df: DataFrame с категориями  
            product_category_relations_df: DataFrame со связями
        Returns:
            Объединенный DataFrame с колонками: product_name, category_name
        """
        self.logger.info("Creating unified result DataFrame")
        
        result = self.analyze_product_categories(
            products_df, categories_df, product_category_relations_df
        )
        
        # Приводим продукты без категорий к тому же формату
        products_without_categories_formatted = result.products_without_categories.select(
            col("product_name"),
            lit(None).cast("string").alias("category_name")
            # cоздаёт новую колонку category_name и заполняет её значением None.
        )
        
        # Объединяем результаты
        unified_result = result.product_category_pairs.select(
            "product_name", "category_name"
        ).union(products_without_categories_formatted) # ставит одну таблицу под другой
        
        unified_count = unified_result.count()
        self.logger.info(f"Unified result created with {unified_count} records")
        
        return unified_result
    
    def _validate_input_dataframes(
        self,
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> None:
        """Валидирует входные DataFrame."""
        self.logger.info("Validating input DataFrames")
        
        self.validator.validate_required_columns(
            products_df, ["product_id", "product_name"], "Products DataFrame"
        )
        self.validator.validate_required_columns(
            categories_df, ["category_id", "category_name"], "Categories DataFrame"
        )
        self.validator.validate_required_columns(
            product_category_relations_df, 
            ["product_id", "category_id"], 
            "Product-Category Relations DataFrame"
        )
    
    def _get_product_category_pairs(
        self,
        products_df: DataFrame,
        categories_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> DataFrame:
        """
        Получает все пары продукт-категория через JOIN операции.
        Returns:
            DataFrame с парами продукт-категория
        """
        self.logger.info("Getting product-category pairs")
        
        # Соединяем связи с продуктами
        product_relations = product_category_relations_df.join(
            products_df.select("product_id", "product_name"),
            on="product_id",
            how="inner" # совпадение в обеих таблицах
        )
        
        # Соединяем результат с категориями
        product_category_pairs = product_relations.join(
            categories_df.select("category_id", "category_name"),
            on="category_id",
            how="inner"
        ).select("product_name", "category_name")
        
        pairs_count = product_category_pairs.count()
        self.logger.info(f"Found {pairs_count} product-category pairs")
        
        return product_category_pairs
    
    def _get_products_without_categories(
        self,
        products_df: DataFrame,
        product_category_relations_df: DataFrame
    ) -> DataFrame:
        """
        Находит продукты, которые не имеют связанных категорий.
        Returns:
            DataFrame с продуктами без категорий
        """
        self.logger.info("Getting products without categories")
        
        # Левое внешнее соединение для поиска продуктов без категорий
        products_without_categories = products_df.join(
            product_category_relations_df.select("product_id").distinct(), # только уникальные строки
            on="product_id",
            how="left_anti" # только те строки из products_df, для которых НЕ нашлось совпадения по ключу product_id в relations_df
        ).select("product_name")
        
        no_category_count = products_without_categories.count()
        self.logger.info(f"Found {no_category_count} products without categories")
        
        return products_without_categories
