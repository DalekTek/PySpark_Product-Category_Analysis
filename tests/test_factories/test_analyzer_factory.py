"""Тесты для фабрики анализаторов"""

from src.factories.analyzer_factory import AnalyzerFactory
from src.analyzers.product_category_analyzer import ProductCategoryAnalyzer
from src.interfaces.analyzer_interface import IProductCategoryAnalyzer


class TestAnalyzerFactory:
    """Тесты для AnalyzerFactory."""
    
    def test_create_product_category_analyzer(self):
        """Тест создания анализатора продуктов и категорий."""
        analyzer = AnalyzerFactory.create_product_category_analyzer()
        
        assert isinstance(analyzer, ProductCategoryAnalyzer)
        assert isinstance(analyzer, IProductCategoryAnalyzer)
    
    def test_analyzer_has_validator(self):
        """Тест того, что анализатор содержит валидатор."""
        analyzer = AnalyzerFactory.create_product_category_analyzer()
        
        assert hasattr(analyzer, 'validator')
        assert analyzer.validator is not None