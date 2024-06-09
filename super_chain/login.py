from omspy_brokers.profitmart import Profitmart
from traceback import print_exc


def get_token(args):
    try:
        O_PROFITMART = Profitmart(**args)
        if O_PROFITMART.authenticate():
            return O_PROFITMART
    except Exception as e:
        print(e)
        print_exc()
