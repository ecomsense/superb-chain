from omspy_brokers.profitmart import Profitmart
from traceback import print_exc
from constants import O_CNFG


def get_token():
    try:
        O_PROFITMART = Profitmart(**O_CNFG)
        if O_PROFITMART.authenticate():
            return O_PROFITMART
    except Exception as e:
        print(e)
        print_exc()


class Helper:
    api = None

    @classmethod
    def login(cls):
        if not Helper.api:
            Helper.api = get_token()
