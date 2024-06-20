import pandas as pd
from constants import S_DATA, O_FUTL

dct_sym = {
    "NIFTY": {
        "diff": 50,
        "index": "Nifty 50",
        "exch": "NSE",
        "token": "26000",
        "depth": 16,
    },
    "BANKNIFTY": {
        "diff": 100,
        "index": "Nifty Bank",
        "exch": "NSE",
        "token": "26009",
        "depth": 25,
    },
    "MIDCPNIFTY": {
        "diff": 100,
        "index": "NIFTY MID SELECT",
        "exch": "NSE",
        "token": "26074",
        "depth": 21,
    },
    "FINNIFTY": {
        "diff": 50,
        "index": "Nifty Fin Services",
        "exch": "NSE",
        "token": "26037",
        "depth": 16,
    },
}


class Symbols:
    def __init__(self, exch: str, symbol: str, expiry: str):
        self.exch = exch
        self.symbol = symbol
        self.expiry = expiry
        self.csvfile = f"{S_DATA}/{symbol}/map_{symbol.lower()}.csv"

    def dump(self):
        if O_FUTL.is_file_not_2day(self.csvfile):
            url = f"https://api.shoonya.com/{self.exch}_symbols.txt.zip"
            df = pd.read_csv(url)
            # filter the response
            df = df[
                (df["Exchange"] == self.exch) & (df["Symbol"] == self.symbol)
                # & (df["TradingSymbol"].str.contains(self.symbol + self.expiry))
            ][["Token", "TradingSymbol"]]
            # split columns with necessary values
            df[["Symbol", "Expiry", "OptionType", "StrikePrice"]] = df[
                "TradingSymbol"
            ].str.extract(r"([A-Z]+)(\d+[A-Z]+\d+)([CP])(\d+)")
            df.to_csv(self.csvfile, index=False)

    def find_symbol_token(self):
        df = pd.read_csv(self.csvfile)
        dct = dict(zip(df["TradingSymbol"], df["Token"]))
        return dct

    def calc_atm_from_ltp(self, ltp) -> int:
        current_strike = ltp - (ltp % dct_sym[self.symbol]["diff"])
        next_higher_strike = current_strike + dct_sym[self.symbol]["diff"]
        if ltp - current_strike < next_higher_strike - ltp:
            return int(current_strike)
        return int(next_higher_strike)

    def build_chain(self, strike):
        df = pd.read_csv(self.csvfile)
        lst = []
        lst.append(self.symbol + self.expiry + "C" + str(strike))
        lst.append(self.symbol + self.expiry + "P" + str(strike))
        for v in range(1, dct_sym[self.symbol]["depth"]):
            lst.append(
                self.symbol
                + self.expiry
                + "C"
                + str(strike + v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "P"
                + str(strike + v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "C"
                + str(strike - v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "P"
                + str(strike - v * dct_sym[self.symbol]["diff"])
            )

        df["Exchange"] = self.exch
        tokens_found = (
            df[df["TradingSymbol"].isin(lst)]
            .assign(tknexc=df["Exchange"] + "|" + df["Token"].astype(str))[
                ["tknexc", "TradingSymbol"]
            ]
            .set_index("tknexc")
        )
        dct = tokens_found.to_dict()
        return dct["TradingSymbol"]

    def find_option(self, atm, c_or_p, distance):
        away = distance * dct_sym[self.symbol]["diff"]
        if c_or_p == "P":
            away = -1 * away
        return f"{self.symbol}{self.expiry}{c_or_p}{atm + away}"


if __name__ == "__main__":
    from constants import logging, O_SETG

    SYMBOL = "BANKNIFTY"
    try:
        symbols = Symbols("NFO", SYMBOL, O_SETG[SYMBOL]["expiry"])
        symbols.get_exchange_token_map_finvasia()
        dct = symbols.get_all_tokens_from_csv()
        print(dct["BANKNIFTY08MAY24C48000"])

        atm = symbols.get_atm(48000.50)
        resp = symbols.find_option(atm, "P", -2)
        print(f"{resp=}")
    except Exception as e:
        logging.debug(f"{e} while getting symbols")
