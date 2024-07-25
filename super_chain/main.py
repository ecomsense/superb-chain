from constants import O_SETG, logging
from api import Helper
from toolkit.kokoo import timer
from symbols import Symbols, dct_sym
from wserver import Wserver
from gsheet_helper import authenticate_gsheet, update_gsheet
import pandas as pd
from collections import defaultdict
import re
from datetime import datetime, timedelta
from GetIVGreeks import DayCountType, ExpType, TryMatchWith, CalcIvGreeks
import json

# initial values
sheet_name = ''
RefreshRate_inSecond = 10
dct = {"BANKNIFTY": {"expiry": "24JUL24", "futExpiry":"31JUL24"},
       "NIFTY": {"expiry": "25JUL24","futExpiry":"25JUL24"},
       "MIDCPNIFTY": {"expiry": "22JUL24","futExpiry":"29JUL24"},
       "FINNIFTY": {"expiry": "23JUL24","futExpiry":"30JUL24"},
       "NIFTYNXT50": {"expiry": "26JUL24","futExpiry":"26JUL24"},
       "SENSEX": {"expiry": "25JUL24","futExpiry":"26JUL24"}
     }

def get_token_map_old():
    dct_map = {}
    for sym in lst:
        if sym != 'SENSEX' and sym != 'BANKEX':
            o_sym = Symbols("NFO", sym, dct[sym]["expiry"], dct[sym]["futExpiry"])
            # dump and save to file
            o_sym.dump()

            # for index tokens are in a dict from symbols
            resp = Helper.api.scriptinfo("NSE", dct_sym[sym]["token"])
            atm = o_sym.calc_atm_from_ltp(float(resp["lp"]))
            dct[sym]['atm'] = atm

            # key is finvasia wserver subscription format
            dct_map.update(o_sym.build_chain(atm))

            #Update index in token list
            dct_map.update(o_sym.updateIndex(sym, dct_sym[sym]["token"], "NSE"))

            #Update index Fut in token list
            dct_map.update(o_sym.updateFut())
        
    return dct_map

def get_token_map():
    dct_map = {}
    for key, values in dct.items():
        if values['sym'] != 'SENSEX' and values['sym'] != 'BANKEX':
            o_sym = Symbols("NFO", values['sym'], values["expiry"], values["futExpiry"])
            o_sym.dump()

            # for index tokens are in a dict from symbols
            resp = Helper.api.scriptinfo("NSE", dct_sym[values['sym']]["token"])
            atm = o_sym.calc_atm_from_ltp(float(resp["lp"]))
            dct[key]['atm'] = atm

            # key is finvasia wserver subscription format
            dct_map.update(o_sym.build_chain(atm))

            #Update index in token list
            dct_map.update(o_sym.updateIndex(values['sym'], dct_sym[values['sym']]["token"], "NSE"))

            #Update index Fut in token list
            dct_map.update(o_sym.updateFut())
        
    return dct_map

def get_token_map_bse(dct_map):
    for sym in lst:
        if sym == 'SENSEX' or sym == 'BANKEX':
            o_sym = Symbols("BFO", sym, dct[sym]["expiry"], dct[sym]["futExpiry"])
            # dump and save to file
            o_sym.dump_bse()

            # for index tokens are in a dict from symbols
            resp = Helper.api.scriptinfo("BSE", dct_sym[sym]["token"])
            atm = o_sym.calc_atm_from_ltp(float(resp["lp"]))

            # key is finvasia wserver subscription format
            dct_map.update(o_sym.build_chain_bse(atm))

            #Update index in token list
            dct_map.update(o_sym.updateIndex(sym, dct_sym[sym]["token"],"BSE"))

            #Update index Fut in token list
            # dct_map.update(o_sym.updateFut_bse())
        
    return dct_map

def run(Ws):
    while not Ws.socket_opened:
        timer(5)
        print("waiting for socket to be opened")

    while True:
        logging.info("socket data")
        logging.info(Ws.ltp)
        updateDF (Ws.ltp)
        timer(RefreshRate_inSecond)

def updateDF(data):
    
    indexData = {}

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
            
    # Process each index separately
    for instrument,expiries in grouped_indexdata.items():
        now = datetime.now()
        curr_timestamp = now.strftime('%d-%m-%Y %H:%M:%S')

        for expiry, values in expiries.items():
            indexInstrument = f'{instrument}{expiry}F'
            indexValues = data.get(indexInstrument, [None] * 17)
            indexSym = instrument.replace('NFO:','')
            # date_obj = datetime.strptime(dct[indexSym]["expiry"], '%d%b%y')
            # exp_date = date_obj.strftime('%Y-%m-%d')
            spotValues = data.get('NSE:'+str(indexSym), [None] * 17)
            indexData_df = {
                "Additional Details": ["Symbol", "Expiry", "Updated at", "LTP (spot, future)", "Open (spot, future)", "High (spot, future)", "Low (spot, future)", "Close (spot, future)", "Future OI"],
                "CE": [indexSym, 0, curr_timestamp, spotValues[0], spotValues[5], spotValues[6], spotValues[7], spotValues[8], int(indexValues[15])],
                "PE": ["", "", "", indexValues[0], indexValues[5], indexValues[6], indexValues[7], indexValues[8], ""]
            }

            indexData[indexSym] = [spotValues[0], indexValues[0], indexData_df]

    # Process each instrument and expiry separately
    
    for instrument, expiries in grouped_data.items():
        for expiry, strikes in expiries.items():
            strike_prices = sorted(strikes.keys())
            combined_rows = []
            combined_values = []

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
            final_df = final_df.fillna("")

            ## Greek calculations
            indexSym = instrument.replace("NFO:", "")
            expDate = datetime.strptime(expiry, "%d%b%y")
            delta = timedelta(days=6)
            fromDate = expDate - delta
            fromDate = fromDate.replace(hour=15, minute=30, second=0)
            for c, d in dct.items():
                if d['sym'] == indexSym and d['expiry'] == expiry:
                    sheet_index = d['SheetName']
                    atmStrike = d['atm']
                    break
            atmCall = final_df.loc[final_df['Strike'] == atmStrike, 'CE LTP'].values[0]
            atmPut = final_df.loc[final_df['Strike'] == atmStrike, 'PE LTP'].values[0]

            IvGreeks = CalcIvGreeks(
                SpotPrice = indexData[indexSym][0],
                FuturePrice = indexData[indexSym][1],
                AtmStrike = atmStrike,
                AtmStrikeCallPrice = atmCall,
                AtmStrikePutPrice = atmPut,
                ExpiryDateTime = expDate,
                ExpiryDateType = ExpType.WEEKLY,
                FromDateTime = fromDate,
                tryMatchWith = TryMatchWith.NSE,
                dayCountType = DayCountType.CALENDARDAYS,
            )
            df_input = pd.DataFrame({
                "StrikePrice": final_df['Strike'],
                "StrikeCallPrice": final_df['CE LTP'],
                "StrikePutPrice": final_df['PE LTP']
                })
            df_output = IvGreeks.GetImpVolAndGreeksFromDF(df_input)
            final_df['CE_Delta'] = df_output['CallDelta']
            final_df['PE_Delta'] = df_output['PutDelta']
            final_df['CE_Gamma'] = df_output['Gamma']
            final_df['PE_Gamma'] = df_output['Gamma']
            final_df['CE_Theta'] = df_output['Theta']
            final_df['PE_Theta'] = df_output['Theta']
            final_df['CE_Vega'] = df_output['Vega']
            final_df['PE_Vega'] = df_output['Vega']
            final_df['CE_Rho'] = df_output['RhoCall']
            final_df['PE_Rho'] = df_output['RhoPut']
            final_df['CE_IV'] = df_output['CallIV']
            final_df['PE_IV'] = df_output['PutIV']

            ## Ordering as per given order
            new_column_order = [
                "CE_Delta",
                "CE_Gamma",
                "CE_Theta",
                "CE_Vega",
                "CE_Rho",
                "CE_OI",
                "CE Change in OI",
                "CE Volume",
                "CE_IV",
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
                "PE_IV",
                "PE Volume",
                "PE Change in OI",
                "PE_OI",
                "PE_Rho",
                "PE_Vega",
                "PE_Theta",
                "PE_Gamma",
                "PE_Delta"
            ]
            final_df = final_df[new_column_order]

            indexData_df = indexData[indexSym][2]
            exp_dateforOption = expDate.strftime('%Y-%m-%d')
            indexData_df ['CE'][1] = exp_dateforOption
            indexDf = pd.DataFrame(indexData_df)  
            spacer = pd.DataFrame({'': [''] * len(final_df)})
            final_df2 = pd.concat([indexDf, spacer, final_df], axis=1)
            final_df2 = final_df2.fillna("")
            start_cell1 = "A1"
            
            update_gsheet(client, sheet_name, sheet_index, final_df2, start_cell1)
            print ('updated option chain in gsheet for ' + str(indexSym) + ' '+str(expiry))
   
def main():

    ## gsheet authenticate
    global client
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_file = "../../gsheet.config.json"
    config_file = "../../optionsConfig.json"
    client = authenticate_gsheet(creds_file, scope)
    checkOptionsConfig(config_file)

    # can be called as a one liner
    Helper.login()

    # get key values for subscription
    dct_map = get_token_map()
    # dct_map = {}
    # dct_map = get_token_map_bse(dct_map)
    logging.info("dct map is "+ str(dct_map))
    # init wsocket
    Ws = Wserver(Helper.api._broker, dct_map)
    run(Ws)

def convertDates(sDate):
    date_obj = datetime.strptime(sDate, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d%b%y").upper()
    return formatted_date

def checkOptionsConfig(filepath):
    global sheet_name
    global dct
    global RefreshRate_inSecond

    with open(filepath) as f:
        d = json.load(f)

    sheet_name = d['sheet_name']
    RefreshRate_inSecond = d['RefreshRate_inSecond']
    optionDataConfig = d['OptionChain']
    dct = {}
    for i in range(0, len(optionDataConfig)):
        dct[str(i)] = {}
        dct[str(i)]['sym'] = optionDataConfig[i]['Symbol']
        dct[str(i)]['expiry'] = convertDates(optionDataConfig[i]['Expiry'])
        dct[str(i)]['futExpiry'] = convertDates(optionDataConfig[i]['FutExpiry'])
        dct[str(i)]['SheetName'] = optionDataConfig[i]['SheetName']
    
    # dct = {
    #   "BANKNIFTY": {"sym": "BANKNIFTY", "expiry": "24JUL24", "futExpiry":"31JUL24", "SheetName":"Weekly_NIFTY"},
    #    "BANKNIFTY1": {"sym": "BANKNIFTY", "expiry": "31JUL24", "futExpiry":"31JUL24", "SheetName":"Weekly_BANKNIFTY"},
    #    "NIFTY": {"sym": "NIFTY", "expiry": "25JUL24","futExpiry":"25JUL24"},
    #    "MIDCPNIFTY": {"sym": "MIDCPNIFTY", "expiry": "22JUL24","futExpiry":"29JUL24"},
    #    "FINNIFTY": {"sym": "FINNIFTY", "expiry": "23JUL24","futExpiry":"30JUL24"},
    #    "NIFTYNXT50": {"sym": "NIFTYNXT50", "expiry": "26JUL24","futExpiry":"26JUL24"}
    #  }
    

main()

