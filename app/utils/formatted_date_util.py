from datetime import datetime


def formatted_datetime():
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
