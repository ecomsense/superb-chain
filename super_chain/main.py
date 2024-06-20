from constants import O_SETG
from api import Helper
from toolkit.kokoo import timer
from symbols import Symbols, dct_sym
from wserver import Wserver


# these will come from settings later
lst = ["BANKNIFTY"]
dct = {"BANKNIFTY": {"expiry": "26JUN24"}}


def get_token_map():
    dct_map = {}
    for sym in lst:
        o_sym = Symbols("NFO", sym, dct[sym]["expiry"])
        # dump and save to file
        o_sym.dump()
        # for index tokens are in a dict from symbols
        resp = Helper.api.scriptinfo("NSE", dct_sym[sym]["token"])
        atm = o_sym.calc_atm_from_ltp(float(resp["lp"]))
        # key is finvasia wserver subscription format
        dct_map.update(o_sym.build_chain(atm))
    return dct_map


def run(Ws):
    while not Ws.socket_opened:
        timer(5)
        print("waiting for socket to be opened")

    while True:
        print(Ws.ltp)
        timer(5)


def main():
    # setting class variable api so any time api
    # can be called as a one liner
    Helper.login()
    # get key values for subscription
    dct_map = get_token_map()
    # init wsocket
    Ws = Wserver(Helper.api._broker, dct_map)
    run(Ws)


main()
