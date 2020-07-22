""" this file is used to define constants, which are used in this project."""
import enum


class SparkStream(enum.Enum):
    """
    This is used to provide constants for spark streaming.
    """
    TCP_IP = "localhost"
    TCP_PORT = 9021
    STREAM_INTERVAL = 2
    APP_NAME = "TwitterStreamApp"
    CHECKPOINT = "checkpoint_TwitterApp"
