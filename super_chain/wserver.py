from constants import logging
from typing import Dict
import time


class Wserver:
    # flag to tell us if the websocket is open
    socket_opened = False
    ltp = {}

    def __init__(self, broker, dct: Dict):
        """
        input:
            broker: broker object
            dct: exchange | token as key
            and tradingsymbol as value
        """
        self.api = broker
        self.tokens = list(dct.keys())
        self.dct_tokens = dct

        ret = self.api.start_websocket(
            order_update_callback=self.event_handler_order_update,
            subscribe_callback=self.event_handler_quote_update,
            socket_open_callback=self.open_callback,
        )
        if ret:
            logging.debug(f"{ret} ws started")

    def open_callback(self):
        self.socket_opened = True
        logging.info("app is connected")
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
            key = message["e"] + "|" + message["tk"]
            tradingsymbol = self.dct_tokens[key]
            exch_sym = message["e"] + ":" + tradingsymbol

            # Initialize the list if it doesn't exist
            if exch_sym not in self.ltp:
                self.ltp[exch_sym] = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]


            # Update the list only if the corresponding keys exist in the message
            self.ltp[exch_sym][0] = float(message.get("lp", self.ltp[exch_sym][0]))
            self.ltp[exch_sym][5] = float(message.get("o", self.ltp[exch_sym][5])) if message.get("o") else self.ltp[exch_sym][5]
            self.ltp[exch_sym][6] = float(message.get("h", self.ltp[exch_sym][6])) if message.get("h") else self.ltp[exch_sym][6]
            self.ltp[exch_sym][7] = float(message.get("l", self.ltp[exch_sym][7])) if message.get("l") else self.ltp[exch_sym][7]
            self.ltp[exch_sym][8] = float(message.get("c", self.ltp[exch_sym][8])) if message.get("c") else self.ltp[exch_sym][8]
            self.ltp[exch_sym][1] = float(message.get("bq1", self.ltp[exch_sym][1])) if message.get("bq1") else self.ltp[exch_sym][1]   #buy quantity
            self.ltp[exch_sym][2] = float(message.get("bp1", self.ltp[exch_sym][2])) if message.get("bp1") else self.ltp[exch_sym][2]   #buy price
            self.ltp[exch_sym][3] = float(message.get("sp1", self.ltp[exch_sym][3])) if message.get("sp1") else self.ltp[exch_sym][3]  # sell price
            self.ltp[exch_sym][4] = float(message.get("sq1", self.ltp[exch_sym][4])) if message.get("sq1") else self.ltp[exch_sym][4]  # sell quantity
            self.ltp[exch_sym][9] = float(message.get("tbq", self.ltp[exch_sym][9])) if message.get("tbq") else self.ltp[exch_sym][9]  # total buy quantity
            self.ltp[exch_sym][10] = float(message.get("tsq", self.ltp[exch_sym][10])) if message.get("tsq") else self.ltp[exch_sym][10]  # total sell quantity
            self.ltp[exch_sym][11] = float(message.get("ap", self.ltp[exch_sym][11])) if message.get("ap") else self.ltp[exch_sym][11]  # avg price
            self.ltp[exch_sym][12] = float(message.get("pc", self.ltp[exch_sym][12])) if message.get("pc") else self.ltp[exch_sym][12]  # percent change
            self.ltp[exch_sym][13] = float(message.get("v", self.ltp[exch_sym][13])) if message.get("v") else self.ltp[exch_sym][13]  # volume
            self.ltp[exch_sym][14] = float(message.get("poi", self.ltp[exch_sym][14])) if message.get("poi") else self.ltp[exch_sym][14]  # prev oi
            self.ltp[exch_sym][15] = float(message.get("oi", self.ltp[exch_sym][15])) if message.get("oi") else self.ltp[exch_sym][15]    #oi
