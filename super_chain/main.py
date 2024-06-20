from constants import O_CNFG, O_SETG
from login import get_token
from symbols import Symbols


def init():
    # this will come from settings later
    lst = ["BANKNIFTY"]
    dct = {"BANKNIFTY": {"expiry": "26JUN24"}}

    Api = get_token(O_CNFG)
    for sym in lst:
        o_sym = Symbols("NFO", sym, dct[sym]["expiry"])
        o_sym.dump()
        ltp = o_sym.calc_atm_from_ltp(50000)
        tokens = o_sym.build_chain(ltp)
        print(tokens)


init()
