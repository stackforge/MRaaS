import logging
import sys

logging.basicConfig(format='[%(asctime)-15s] %(message)s')

# root logger. change level to get info, debug from libraries.
logging.getLogger().setLevel(logging.WARN)

# application logger. change level for app-level logs.
logger = logging.getLogger("hadoop")
logger.setLevel(logging.DEBUG)
for handler in logger.handlers:
    logger.removeHandler(handler)


# return the first element in the list which satisfies the lambda, if one exists.
def select(lst, test):
    for x in lst:
        if test(x): return x
    return None
