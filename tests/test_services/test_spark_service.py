"""Тесты для сервиса Spark"""

import pytest
from src.services.spark_service import SparkService
from pyspark.sql import SparkSession


class TestSparkService:
    def test_singleton_pattern(self):
        """Тест паттерна Singleton"""
        service1 = SparkService()
        service2 = SparkService()
        
        assert service1 is service2

    def test_get_spark_session(self, spark_session: SparkSession):
        """Тест получения Spark сессии"""
        service = SparkService()
        # Вызываем get_spark_session. Так как сессия уже создана фикстурой,
        # этот вызов просто вернет существующую.
        session = service.get_spark_session("TestProductCategoryAnalysis")

        assert session is not None
        # Проверяем, что имя приложения соответствует тому,
        # которое было задано в conftest.py
        assert session.sparkContext.appName == "TestProductCategoryAnalysis"
    
    def test_spark_session_reuse(self):
        """Тест переиспользования Spark сессии"""
        service = SparkService()
        session1 = service.get_spark_session("TestApp1")
        session2 = service.get_spark_session("TestApp2")
        
        # Должна возвращаться та же сессия
        assert session1 is session2
