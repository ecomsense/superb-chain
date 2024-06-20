from constants import logging


class Wserver:
    # flag to tell us if the websocket is open
    socket_opened = False
    ltp = {}

    def __init__(self, broker, tokens, dct_tokens):
        self.api = broker
        self.tokens = tokens
        self.dct_tokens = dct_tokens
        ret = self.api.start_websocket(
            order_update_callback=self.event_handler_order_update,
            subscribe_callback=self.event_handler_quote_update,
            socket_open_callback=self.open_callback,
        )
        if ret:
            logging.debug(f"{ret} ws started")

    def open_callback(self):
        self.socket_opened = True
        print("app is connected")
        self.api.subscribe(self.tokens, feed_type="d")
        # api.subscribe(['NSE|22', 'BSE|522032'])

    # application callbacks
    def event_handler_order_update(self, message):
        logging.info("order event: " + str(message))

    def event_handler_quote_update(self, message):
        # e   Exchange
        # tk  Token
        # lp  LTP
        # pc  Percentage change
        # v   volume
        # o   Open price
        # h   High price
        # l   Low price
        # c   Close price
        # ap  Average trade price
        #
        logging.debug(
            "quote event: {0}".format(time.strftime("%d-%m-%Y %H:%M:%S")) + str(message)
        )
        val = message.get("lp", False)
        if val:
            exch_tkn = message.get("e", "") + "|" + message.get("tk", "")
            token = self.dct_tokens.get(exch_tkn, None)

            if token is not None:
                # Initialize the list if it doesn't exist
                if token not in self.ltp:
                    self.ltp[token] = [None, None, None, None]

                # Update the list only if the corresponding keys exist in the message
                self.ltp[token][0] = float(message.get("o", self.ltp[token][0]))
                self.ltp[token][1] = float(message.get("h", self.ltp[token][1]))
                self.ltp[token][2] = float(message.get("l", self.ltp[token][2]))
                self.ltp[token][3] = float(message.get("lp", self.ltp[token][3]))


if __name__ == "__main__":
    import time
    import pandas as pd

    """
    wserver = Wserver(
        broker,
        lst,
        dct,
    )
    dct_quotes = False
    while not dct_quotes:
        dct_quotes = wserver.ltp
        time.sleep(1)
    dct_quotes = {k: v
                  for k, v in dct_quotes.items()
                  if (v[0] == v[1]) or (v[0] == v[2])}
    """
    dct_quotes = {
        "AUROPHARMA": ["1099.90", "1099.90", "1062.80", "1066.40"],
        "HINDPETRO": ["375.60", "375.60", "364.00", "369.30"],
        "INTELLECT": ["780.90", "814.70", "780.90", "811.70"],
        "LAURUSLABS": ["397.15", "422.00", "397.15", "414.55"],
        "MANAPPURAM": ["170.65", "177.55", "170.65", "176.65"],
        "MCDOWELL-N": ["1061.00", "1084.50", "1061.00", "1076.00"],
        "MGL": ["1203.00", "1203.00", "1184.40", "1194.70"],
        "NATIONALUM": ["112.45", "117.80", "112.45", "115.10"],
        "PNB": ["90.45", "90.45", "88.30", "89.25"],
        "POLYCAB": ["5633.00", "5633.00", "5305.65", "5359.30"],
        "PVRINOX": ["1764.00", "1764.00", "1694.55", "1700.80"],
        "HINDZINC": ["305.05", "312.00", "305.05", "309.90"],
        "PRESTIGE": ["1074.80", "1122.20", "1074.80", "1114.90"],
        "VBL": ["1291.30", "1291.30", "1225.55", "1238.60"],
    }

    # convert dct_quotes values which is a list into float
    dct_quotes = {k: [float(x) for x in v] for k, v in dct_quotes.items()}
    print(dct_quotes)
    lst = list(dct_quotes.keys())
