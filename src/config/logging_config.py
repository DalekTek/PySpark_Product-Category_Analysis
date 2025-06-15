"""Модуль конфигурации логгирования для проекта"""

import logging
import sys
from typing import Optional


class LoggerConfig:
    """Класс для настройки логгирования приложения."""
    
    @staticmethod
    def setup_logger(
        name: str, 
        level: int = logging.INFO,
        format_string: Optional[str] = None
    ) -> logging.Logger:
        """
        Настраивает и возвращает логгер с заданными параметрами.
        
        Args:
            name: Имя логгера
            level: Уровень логгирования
            format_string: Формат сообщений логгера
            
        Returns:
            Настроенный логгер
        """
        if format_string is None:
            format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # Проверяем, что хендлер еще не добавлен
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            formatter = logging.Formatter(format_string)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger