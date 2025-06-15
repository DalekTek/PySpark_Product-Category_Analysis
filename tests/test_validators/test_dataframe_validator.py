"""Тесты для валидатора DataFrame"""

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.validators.dataframe_validator import DataFrameValidator
from src.exceptions.custom_exceptions import DataFrameValidationError


class TestDataFrameValidator:
    def test_validate_required_columns_success(self, spark_session: SparkSession):
        """Тест успешной валидации колонок"""
        validator = DataFrameValidator()
        df = spark_session.createDataFrame(
            [(1, "test")], ["id", "name"]
        )
        
        # Не должно выбрасывать исключение
        validator.validate_required_columns(df, ["id", "name"])
    
    def test_validate_required_columns_missing(self, spark_session: SparkSession):
        """Тест валидации при отсутствии колонок"""
        validator = DataFrameValidator()
        df = spark_session.createDataFrame(
            [(1, "test")], ["id", "name"]
        )
        
        with pytest.raises(DataFrameValidationError) as exc_info:
            validator.validate_required_columns(df, ["id", "missing_column"])
        
        assert "missing_column" in str(exc_info.value)
    
    def test_validate_none_dataframe(self):
        """Тест валидации None DataFrame"""
        validator = DataFrameValidator()
        
        with pytest.raises(DataFrameValidationError) as exc_info:
            validator.validate_required_columns(None, ["id"])
        
        assert "cannot be None" in str(exc_info.value)
    
    def test_validate_non_empty(self, spark_session: SparkSession):
        """Тест валидации непустого DataFrame"""
        validator = DataFrameValidator()
        
        # Непустой DataFrame
        df = spark_session.createDataFrame([(1, "test")], ["id", "name"])
        # Не должно выбрасывать исключение
        validator.validate_non_empty(df)

        # Пустой DataFrame - должен только предупредить, но не выбрасывать исключение
        # Явно указываем схему, чтобы избежать ошибки CANNOT_INFER_EMPTY_SCHEMA
        empty_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        empty_df = spark_session.createDataFrame([], schema=empty_schema)

        validator.validate_non_empty(empty_df)  # Только warning
