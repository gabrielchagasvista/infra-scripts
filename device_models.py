import pandas as pd
from sheets import read_sheets

# Caminho para o arquivo CSV
jc_device_models = 'jc_device_models.csv'

# ID da planilha
id_base_jc = '1jTEL86mE80AjDCoZ6C63MOe4-jK5b9_vezdpVewxdGo'

# Lê o arquivo CSV e seleciona as colunas desejadas na planilha
dfdevice_models = pd.read_csv(jc_device_models)
dfbaseJC = read_sheets(id_base_jc)[['resource_object_id', 'device_os', 'email', 'username']]

# Realiza a junção dos DataFrames
dfresult = dfdevice_models.merge(dfbaseJC, left_on='SystemId', right_on='resource_object_id', how='left')

# Aplica o filtro nas linhas onde 'username' não começa com os valores especificados
dfresult = dfresult[~dfresult['username'].astype(str).str.startswith(('userloftmac.local', 'admgloft'))]

# Remove as linhas onde 'email' está vazio
dfresult = dfresult.dropna(subset=['email'])

# Remove as colunas especificadas
dfresult.drop(columns=['username', 'resource_object_id', 'SystemId'], inplace=True)

# Salva o DataFrame resultante em um arquivo CSV
dfresult.to_csv('export.csv', index=False)
