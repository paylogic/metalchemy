"""Entry point for API."""
__version__ = '1.0.0'

try:
    from metalchemy.metadata import initialize

    __all__ = ('__version__', initialize.__name__)
except ImportError:
    pass
