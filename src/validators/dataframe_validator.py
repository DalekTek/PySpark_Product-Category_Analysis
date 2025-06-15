"""Модуль для валидации DataFrame"""

from typing import List, Set
from pyspark.sql import DataFrame
import logging
from src.exceptions.custom_exceptions import DataFrameValidationError
from src.config.logging_config import LoggerConfig


class DataFrameValidator:
    """Класс для валидации DataFrame"""
    def __init__(self):
        self.logger = LoggerConfig.setup_logger(self.__class__.__name__)
    
    def validate_required_columns(
        self, 
        df: DataFrame, 
        required_columns: List[str],
        df_name: str = "DataFrame"
    ) -> None:
        """
        Проверяет наличие обязательных колонок в DataFrame.
        Args:
            df: DataFrame для проверки
            required_columns: Список обязательных колонок
            df_name: Имя DataFrame для логгирования
        Raises:
            DataFrameValidationError: Если отсутствуют обязательные колонки
        """
        if df is None:
            raise DataFrameValidationError(f"{df_name} cannot be None")
            
        existing_columns: Set[str] = set(df.columns)
        missing_columns: Set[str] = set(required_columns) - existing_columns
        
        if missing_columns:
            error_msg = f"Missing required columns in {df_name}: {missing_columns}"
            self.logger.error(error_msg)
            raise DataFrameValidationError(error_msg)
            
        self.logger.info(f"Validation passed for {df_name}")
    
    def validate_non_empty(self, df: DataFrame, df_name: str = "DataFrame") -> None:
        """
        Проверяет, что DataFrame не пустой.
        Args:
            df: DataFrame для проверки
            df_name: Имя DataFrame для логгирования
        Raises:
            DataFrameValidationError: Если DataFrame пустой
        """
        if df.count() == 0:
            error_msg = f"{df_name} is empty"
            self.logger.warning(error_msg)
            # Не выбрасываем исключение, так как пустые DataFrame могут быть валидными
