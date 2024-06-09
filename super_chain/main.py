from constants import O_CNFG, O_SETG
from login import get_token


def init():
    API = get_token(O_CNFG)
    positions = API.positions
    print(f"{positions=}")

    pass


init()
