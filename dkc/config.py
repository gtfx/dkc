import sys
import os.path

_CONFIGURATION = {
    'global': {
        'check_interval': 300,
    },
    'logging': {
        'level': 'DEBUG'
    },
    'kinesis': {
        'input_hwm': '80',
        'input_lwm': '30',
        'output_hwm': '80',
        'output_lwm': '30',
        'input_per_shard': 1048576,
        'output_per_shard': 2097152,
    }
}


def get_global_option(option):
    """ Returns the value of the option

    :returns: str or None
    """
    try:
        return _CONFIGURATION['global'][option]
    except KeyError:
        return None


def get_logging_option(option):
    """ Returns the value of the option

    :returns: str or None
    """
    try:
        return _CONFIGURATION['logging'][option]
    except KeyError:
        return None

def get_kinesis_option(option):
    try:
        return _CONFIGURATION['kinesis'][option]
    except KeyError:
        return None

