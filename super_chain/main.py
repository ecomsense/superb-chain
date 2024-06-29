from constants import O_SETG, logging
from api import Helper
from toolkit.kokoo import timer
from symbols import Symbols, dct_sym
from wserver import Wserver
from gsheet_helper import authenticate_gsheet, update_gsheet
import pandas as pd
from collections import defaultdict
import re
from datetime import datetime

# these will come from settings later
sheet_name = 'Test gSheet'
lst = ["BANKNIFTY", "NIFTY", "MIDCPNIFTY"]
dct = {"BANKNIFTY": {"expiry": "03JUL24", "futExpiry":"31JUL24"},
       "NIFTY": {"expiry": "04JUL24","futExpiry":"25JUL24"},
       "MIDCPNIFTY": {"expiry": "01JUL24","futExpiry":"29JUL24"}
     }

def get_token_map():
    dct_map = {}
    for sym in lst:
        o_sym = Symbols("NFO", sym, dct[sym]["expiry"], dct[sym]["futExpiry"])
        # dump and save to file
        o_sym.dump()

        # for index tokens are in a dict from symbols
        resp = Helper.api.scriptinfo("NSE", dct_sym[sym]["token"])
        atm = o_sym.calc_atm_from_ltp(float(resp["lp"]))

        # key is finvasia wserver subscription format
        dct_map.update(o_sym.build_chain(atm))

        #Update index in token list
        dct_map.update(o_sym.updateIndex(sym, dct_sym[sym]["token"]))

        #Update index Fut in token list
        dct_map.update(o_sym.updateFut())
        
    return dct_map


def run(Ws):
    while not Ws.socket_opened:
        timer(5)
        print("waiting for socket to be opened")

    while True:
        logging.info("socket data")
        logging.info(Ws.ltp)
        updateDF (Ws.ltp)
        timer(10)

def updateDF(data):
    
    # To hold grouped data by instrument and expiry
    grouped_data = defaultdict(lambda: defaultdict(dict))
    grouped_indexdata = defaultdict(lambda: defaultdict(dict))

    # Regex to extract expiry date
    expiry_pattern = re.compile(r"(\d{2}[A-Z]{3}\d{2})")

    # Group data by instrument and expiry
    for key, values in data.items():
        match = expiry_pattern.search(key)
        if match:
            expiry = match.group(1)
            instrument, rest = key.split(f"{expiry}")
            strike = rest[1:]
            if (strike):
                grouped_data[instrument][expiry][int(strike)] = values
            else:
                grouped_indexdata[instrument][expiry] = values

    # Process each instrument and expiry separately
    for instrument, expiries in grouped_data.items():
        combined_rows = []
        combined_values = []

        for expiry, strikes in expiries.items():
            strike_prices = sorted(strikes.keys())
            for strike in strike_prices:
                left_key = f"{instrument}{expiry}C{strike}"
                right_key = f"{instrument}{expiry}P{strike}"
                left_values = data.get(left_key, [None] * 17)
                right_values = data.get(right_key, [None] * 17)
                combined_values = left_values[:16] + [strike] + right_values[:16]
                combined_rows.append(combined_values)

        # Create DataFrame columns 
        columns = [
            "CE LTP",
            "CE_BIDQTY",
            "CE_BIDPRICE",
            "CE_ASKPRICE",
            "CE_ASKQTY",
            "CE_OPEN",
            "CE_HIGH",
            "CE_LOW",
            "CE_CLOSE",
            "CE BUY QTY",
            "CE SELL QTY",
            "CE VWAP",
            "CE LTP Change",
            "CE Volume",
            "CE prev OI",
            "CE_OI",
            "Strike",
            "PE LTP",
            "PE_BIDQTY",
            "PE_BIDPRICE",
            "PE_ASKPRICE",
            "PE_ASKQTY",
            "PE_OPEN",
            "PE_HIGH",
            "PE_LOW",
            "PE_CLOSE",
            "PE BUY QTY",
            "PE SELL QTY",
            "PE VWAP",
            "PE LTP Change",
            "PE Volume",
            "PE prev OI",
            "PE_OI",
        ]

        # Create a DataFrame from the combined rows
        final_df = pd.DataFrame(combined_rows, columns=columns)

        ##Change in oi calculation
        final_df["CE Change in OI"] = final_df["CE_OI"] - final_df["CE prev OI"]
        final_df["PE Change in OI"] = final_df["PE_OI"] - final_df["PE prev OI"]
        final_df = final_df.drop(columns=["CE prev OI"])
        final_df = final_df.drop(columns=["PE prev OI"])

        ## Ordering as per given order
        new_column_order = [
            "CE_OI",
            "CE Change in OI",
            "CE Volume",
            "CE LTP Change",
            "CE VWAP",
            "CE BUY QTY",
            "CE SELL QTY",
            "CE_OPEN",
            "CE_HIGH",
            "CE_LOW",
            "CE_CLOSE",
            "CE_BIDQTY",
            "CE_BIDPRICE",
            "CE_ASKPRICE",
            "CE_ASKQTY",
            "CE LTP",
            "Strike",
            "PE LTP",
            "PE_BIDQTY",
            "PE_BIDPRICE",
            "PE_ASKPRICE",
            "PE_ASKQTY",
            "PE_OPEN",
            "PE_HIGH",
            "PE_LOW",
            "PE_CLOSE",
            "PE BUY QTY",
            "PE SELL QTY",
            "PE VWAP",
            "PE LTP Change",
            "PE Volume",
            "PE Change in OI",
            "PE_OI",
        ]
        final_df = final_df[new_column_order]
        final_df = final_df.fillna("")

        indexSym = instrument.replace("NFO:", "")
        start_cell1 = "E1"
        if indexSym == "NIFTY":
            sheet_index = 0
        elif indexSym == "BANKNIFTY":
            sheet_index = 1
        elif (indexSym == 'MIDCPNIFTY'):
            sheet_index = 2
        update_gsheet(client, sheet_name, sheet_index, final_df, start_cell1)
        print ('updated option chain in gsheet for ' + str(indexSym))
    
    for instrument,expiries in grouped_indexdata.items():
        now = datetime.now()
        curr_timestamp = now.strftime('%d-%m-%Y %H:%M:%S')

        for expiry, values in expiries.items():
            indexInstrument = f'{instrument}{expiry}F'
            indexValues = data.get(indexInstrument, [None] * 17)
            indexSym = instrument.replace('NFO:','')
            date_obj = datetime.strptime(dct[indexSym]["expiry"], '%d%b%y')
            exp_date = date_obj.strftime('%Y-%m-%d')
            spotValues = data.get('NSE:'+str(indexSym), [None] * 17)
            indexData_df = {
                "Additional Details": ["Symbol", "Expiry", "Updated at", "LTP (spot, future)", "Open (spot, future)", "High (spot, future)", "Low (spot, future)", "Close (spot, future)", "Future OI"],
                "CE": [indexSym, exp_date, curr_timestamp, spotValues[0], spotValues[5], spotValues[6], spotValues[7], spotValues[8], int(indexValues[15])],
                "PE": ["", "", "", indexValues[0], indexValues[5], indexValues[6], indexValues[7], indexValues[8], ""]
            }
            
            indexDf = pd.DataFrame(indexData_df)            
            start_cell1 = 'A1'
            if (indexSym == 'NIFTY'):
                sheet_index = 0
            elif (indexSym == 'BANKNIFTY'):
                sheet_index = 1
            elif (indexSym == 'MIDCPNIFTY'):
                sheet_index = 2
            update_gsheet(client, sheet_name, sheet_index, indexDf, start_cell1)
            print ('updated Additional values in gsheet for '+str(indexSym))

def main():

    ## gsheet authenticate
    global client
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_file = "../../gsheet.config.json"
    client = authenticate_gsheet(creds_file, scope)

    # setting class variable api so any time api
    # can be called as a one liner
    Helper.login()
    # get key values for subscription
    dct_map = get_token_map()
    logging.info("dct map is "+ str(dct_map))
    # init wsocket
    Ws = Wserver(Helper.api._broker, dct_map)
    run(Ws)


main()
