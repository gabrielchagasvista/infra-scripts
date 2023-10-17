import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
secret_json = 'pass.json'




#credentials = ServiceAccountCredentials.from_json_keyfile_name('pass.json', scope)
#gc = gspread.authorize(credentials)
#
## Abra a planilha pelo seu ID ou URL
#planilha = gc.open_by_url('https://docs.google.com/spreadsheets/d/1braeqbCd6C-rR8CL5mUhh9oAJeC7vWsQ9G-g-6zIEeI/edit#gid=0')
#
## Selecione a guia da planilha que você deseja ler
#guia = planilha.worksheet('page1')
#
## Leitura dos dados para um DataFrame Pandas
#df = pd.DataFrame(guia.get_all_records())
#
## Agora você pode manipular os dados usando o Pandas
#print(df)
