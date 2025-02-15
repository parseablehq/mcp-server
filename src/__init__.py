# src/__init__.py

from .analyzers import PostgresAnalyzer
from .utils import setup_logger

__version__ = '0.1.0'

__all__ = [
    'PostgresAnalyzer',
    'setup_logger',
]