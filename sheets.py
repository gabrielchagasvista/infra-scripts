import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

def google_sheets_credentials():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    secret_json = 'pass.json'
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(secret_json, scope)
        client = gspread.authorize(creds)
        return client
    
    except Exception as e:
        raise Exception(f'Falha ao carregar credenciais do Google Sheets: {repr(e)}')

def read_sheets(ID):
    
    try:
        client = google_sheets_credentials()
        worksheet = client.open_by_key(ID).get_worksheet(0)
        rows = worksheet.get_all_values()
        return pd.DataFrame(data=rows[1:], columns=rows[0])

    except Exception as e:
        raise Exception(f'Falha ao ler os dados da planilha: {repr(e)}')
    
