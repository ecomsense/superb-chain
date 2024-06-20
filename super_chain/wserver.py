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
                self.ltp[exch_sym] = [None, None, None, None]

            # Update the list only if the corresponding keys exist in the message
            self.ltp[exch_sym][0] = float(message.get("o", self.ltp[exch_sym][0]))
            self.ltp[exch_sym][1] = float(message.get("h", self.ltp[exch_sym][1]))
            self.ltp[exch_sym][2] = float(message.get("l", self.ltp[exch_sym][2]))
            self.ltp[exch_sym][3] = float(message.get("lp", self.ltp[exch_sym][3]))
