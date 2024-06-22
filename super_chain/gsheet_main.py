import pandas as pd
import numpy as np
from gsheet_helper import authenticate_gsheet, update_gsheet

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_file = 'gsheet.config.json'
client = authenticate_gsheet(creds_file, scope)

# random DataFrame
df1 = pd.DataFrame(np.random.randint(0, 25, size=(25, 4)), columns=list('ABCD'))
df2 = pd.DataFrame(np.random.randint(0, 25, size=(25, 4)), columns=list('EFGH'))

# Gsheet details
sheet_name = 'Test gSheet'
start_cell1 = 'A5'
sheet_index = 0
update_gsheet(client, sheet_name, sheet_index, df1, start_cell1)

## 2nd sheet update
start_cell2 = 'A31'
update_gsheet(client, sheet_name, sheet_index, df2, start_cell2)