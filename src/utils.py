"""
A collection of helper functions used across the project for:
- String manipulation
- File operations
- Data validation
- Logging and error handling

These utility functions are designed to be generic enough for reuse in different
parts of the codebase.
"""
import logging


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    :param name: Set name of the logger. Useful if there are many loggers are used during the running process.
    :param level: Set the root logger level to the specified level
    :raise ValueError: if name is not specified
    """

    if name is None:
        name = __name__

    formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)s [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(stream_handler)

    return logger