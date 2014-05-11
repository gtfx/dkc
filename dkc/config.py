import sys
import os.path

_CONFIGURATION = {
    'global': {
        'region': 'us-east-1'
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
