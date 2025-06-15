"""Модуль для работы с Spark сессией"""

from typing import Optional
from pyspark.sql import SparkSession
from src.exceptions.custom_exceptions import SparkSessionError
from src.config.logging_config import LoggerConfig


class SparkService:
    """Сервис для управления Spark сессией"""

    _instance: Optional['SparkService'] = None
    _spark_session: Optional[SparkSession] = None

    def __new__(cls) -> 'SparkService':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'logger'):
            self.logger = LoggerConfig.setup_logger(self.__class__.__name__)

    def show_spark_version(self) -> None:
        """Выводит информацию о текущей версии Spark и Hadoop"""

        if self._spark_session is not None:
            self.logger.info(f"PySpark Version: {self._spark_session.version}")
            self.logger.info(
                f"Hadoop Version: {self._spark_session.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")
        else:
            self.logger.warning("No active Spark session found. Creating a new one.")
            self.get_spark_session()
            self.show_spark_version()

    def get_spark_session(
            self,
            app_name: str = "ProductCategoryAnalysis"
    ) -> SparkSession:
        """
        Получает или создает Spark сессию.
        Args:
            app_name: Имя приложения для Spark
        Returns:
            Spark сессия
        Raises:
            SparkSessionError: Если не удается создать сессию
        """
        if self._spark_session is None:
            try:
                self.logger.info(f"Creating Spark session with app name: {app_name}")
                self._spark_session = (SparkSession
                                       .builder
                                       .appName(app_name)
                                       .config("spark.sql.adaptive.enabled", "true")
                                       .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                                       .getOrCreate())
                self.logger.info("Spark session created successfully")
            except Exception as e:
                error_msg = f"Failed to create Spark session: {str(e)}"
                self.logger.error(error_msg)
                raise SparkSessionError(error_msg) from e

        return self._spark_session

    def stop_spark_session(self) -> None:
        """Останавливает Spark сессию"""
        if self._spark_session is not None:
            self.logger.info("Stopping Spark session")
            self._spark_session.stop()
            self._spark_session = None
            self.logger.info("Spark session stopped")
