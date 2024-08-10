import pandas as pd
from constants import S_DATA, O_FUTL
from datetime import datetime

dct_sym = {
     "NIFTY": {
        "diff": 50,
        "index": "Nifty 50",
        "exch": "NSE",
        "token": "26000",
        "depth": 25
    },
    "BANKNIFTY": {
        "diff": 100,
        "index": "Nifty Bank",
        "exch": "NSE",
        "token": "26009",
        "depth": 25
    },
    "MIDCPNIFTY": {
        "diff": 25,
        "index": "NIFTY MID SELECT",
        "exch": "NSE",
        "token": "26074",
        "depth": 25
    },
    "FINNIFTY": {
        "diff": 50,
        "index": "Nifty Fin Services",
        "exch": "NSE",
        "token": "26037",
        "depth": 25
    },
    "NIFTYNXT50":{
        "diff": 100,
        "index": "Nifty Next 50",
        "exch": "NSE",
        "token": "26013",
        "depth": 25
    },
    "SENSEX": {
        "diff": 100,
        "index": "Sensex",
        "exch": "BSE",
        "token": "1",
        "depth": 25,
    },
    "BANKEX": {
        "diff": 100,
        "index": "Bankex",
        "exch": "BSE",
        "token": "12",
        "depth": 25,
    }
}


class Symbols:
    def __init__(self, exch: str, symbol: str, expiry: str, futExpiry: str):
        self.exch = exch
        self.symbol = symbol
        self.expiry = expiry
        self.futExpiry = futExpiry
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
            # df[["Symbol", "Expiry", "OptionType", "StrikePrice"]] = df[
            #     "TradingSymbol"
            # ].str.extract(r"([A-Z]+)(\d+[A-Z]+\d+)([CPF])(\d+)?")

            ## to support nifty next 50 
            df[["Symbol", "Expiry", "OptionType", "StrikePrice"]] = df[
                "TradingSymbol"
            ].str.extract(r"([A-Z]+[A-Z0-9]*)(\d{2}[A-Z]{3}\d{2})([CP])(\d+)")
            df.to_csv(self.csvfile, index=False)

    def dump_bse(self):
        if O_FUTL.is_file_not_2day(self.csvfile):
            url = f"https://api.shoonya.com/{self.exch}_symbols.txt.zip"
            df = pd.read_csv(url)
            sym = self.symbol 
            if (self.symbol == "SENSEX"):
                exchSym = 'BSXOPT'
            elif (self.symbol == "BANKEX"):
                exchSym = 'BKXOPT'
            #expiry = '24809'
            ## Format the date as YYMMDD(24809) and convert to an integer
            # date_obj = datetime.strptime(self.expiry, '%d%b%y')
            # year = date_obj.strftime('%y')  # '24'
            # month = date_obj.strftime('%m')  # '08'
            # day = date_obj.strftime('%d')  # '09'
            expiry = self.expiry
            expression = "("+sym+ ")("+ expiry +")(\d+)([CP]E?|FUT)"
            df1 = df[ (df["Exchange"] == self.exch) & (df["Symbol"] == exchSym) ][["Token", "TradingSymbol"]]
            df1[["ssymbol", "Expiry", "OptionType", "StrikePrice"]] = df1[
                "TradingSymbol"
            ].str.extract(expression)

            ## fut
            if (self.symbol == "SENSEX"):
                exchSym = 'BSXFUT'
            elif (self.symbol == "BANKEX"):
                exchSym = 'BKXFUT'
            expression2 = "("+sym+ ")("+ expiry +")(FUT)"
            df2 = df[ (df["Exchange"] == self.exch) & (df["Symbol"] == exchSym) ][["Token", "TradingSymbol"]]
            df2[["ssymbol", "Expiry", "OptionType"]] = df2[
                "TradingSymbol"
            ].str.extract(expression2)
            result_df = pd.concat([df1, df2], ignore_index=True)
            result_df.to_csv(self.csvfile, index=False)

    #SENSEX502471822900CE  18 jul 22900
    #SENSEX5024AUG25050CE
    #BANKEX2472262200PE
    #BANKNIFTY28AUG24C60100

    def find_token_from_symbol(self, symbol):
        df = pd.read_csv(self.csvfile)
        dct = dict(zip(df["TradingSymbol"], df["Token"]))
        return dct[symbol]

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

    def build_chain_bse(self, strike):
        df = pd.read_csv(self.csvfile)
        lst = []
        sym = self.symbol
        expiry = self.expiry
        lst.append(sym + expiry + str(strike) + "CE")
        lst.append(sym + expiry + str(strike) + "PE")
        for v in range(1, dct_sym[self.symbol]["depth"]):
            lst.append(
                sym
                + expiry
                + str(strike + v * dct_sym[self.symbol]["diff"])
                + "CE"
            )
            lst.append(
                sym
                + expiry
                + str(strike + v * dct_sym[self.symbol]["diff"])
                + "PE"
            )
            lst.append(
                sym
                + expiry
                + str(strike - v * dct_sym[self.symbol]["diff"])
                + "CE"
            )
            lst.append(
                sym
                + expiry
                + str(strike - v * dct_sym[self.symbol]["diff"])
                + "PE"
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

    def updateIndex(self, symbol, token, exchange):

        tokens = str(exchange) +"|"+ str(token)
        val = { tokens: symbol }
        return val

    def updateFut(self):
        df = pd.read_csv(self.csvfile)
        lst = []
        lst.append(self.symbol + self.futExpiry + "F")
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
    
    def updateFut_bse(self):
        df = pd.read_csv(self.csvfile)
        lst = []
        lst.append(self.symbol + self.futExpiry + "FUT")
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

