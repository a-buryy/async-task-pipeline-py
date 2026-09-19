import logging
from importlib.metadata import version

from .pipeline import (
    TaskHandle,
    TaskPipeline,
    current_pipeline,
)


__all__ = [
    'TaskHandle',
    'TaskPipeline',
    'current_pipeline',
]
__version__ = version('async-task-pipeline-py')

logging.getLogger(__name__).addHandler(logging.NullHandler())
