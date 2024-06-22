import gspread
from oauth2client.service_account import ServiceAccountCredentials

def authenticate_gsheet(creds_file, scope):
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)
    return client

def update_gsheet(client, sheet_name,sheet_index, df, start_cell):
    
    sheet = client.open(sheet_name)
    sheet_instance = sheet.get_worksheet(sheet_index)
    sheet_instance.update([df.columns.values.tolist()] + df.values.tolist(), start_cell)