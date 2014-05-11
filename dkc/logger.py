import logging


def get_logger(inst, log_level='INFO'):
    logger = logging.getLogger('%s.%s[%s]' % (inst.__module__, type(inst).__name__, id(inst)))
    logger.setLevel(getattr(logging, log_level))
    logger.addHandler(logging.StreamHandler())
    return logger