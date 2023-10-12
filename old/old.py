import pandas as pd


#base_jc = 'csv-prod/base_jc.csv'
#base_netskope = 'csv-prod/base_netskope.csv'
#base_crowdstrike = 'csv-prod/base_crowdstrike.csv'


base_jc = 'csv-dev/base_jc2.csv'
base_netskope = 'csv-dev/base_netskope.csv'
base_crowdstrike = 'csv-dev/base_crowdstrike.csv'
#

def jc_activated(): 
    dfbaseJC = pd.read_csv(base_jc)
    columns_to_keep = ['email', 'hostname', 'device_os']  # Substitua com os nomes das colunas que deseja manter
    
    # Condition 1: Filtrar por 'user_state' igual a 'ACTIVATED'
    condition1 = dfbaseJC['user_state'] == 'ACTIVATED'
    
    # Condition 2: Filtrar por 'username' que não comece com 'userloftmac.local' ou 'admgloft'
    condition2 = ~(dfbaseJC['username'].astype(str).str.startswith('userloftmac.local') | dfbaseJC['username'].astype(str).str.startswith('admgloft'))
    
    # Aplicar as duas condições
    dfbaseJC_filtered = dfbaseJC[condition1 & condition2][columns_to_keep].copy()
    return dfbaseJC_filtered
def jc_activated(): 
    dfbaseJC = pd.read_csv(base_jc)
    columns_to_keep = ['email', 'hostname', 'device_os']  # Substitua com os nomes das colunas que deseja manter
    dfbaseJC_filtered = dfbaseJC[dfbaseJC['user_state'] == 'ACTIVATED'][columns_to_keep].copy()
    return dfbaseJC_filtered



#def jc_activated(): 
#    dfbaseJC = pd.read_csv(base_jc)
#    columns_to_keep = ['email', 'hostname', 'device_os']  # Substitua com os nomes das colunas que deseja manter
#    
#    # Não filtrar por 'user_state' igual a 'ACTIVATED'
#    dfbaseJC_filtered = dfbaseJC[columns_to_keep].copy()
#    
#    # Adicione outras condições aqui, se necessário
#    
#    return dfbaseJC_filtered


def netskope():
    df_netskope = pd.read_csv(base_netskope)
    dfbaseJC_filtered = jc_activated()
    
    # merge netskope
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    # Cria coluna 'Status Enroll Netskope' # Valor baseado na correspondência do hostname
    df_merge_netskope['Status Enroll Netskope'] = df_merge_netskope.apply(lambda row: 'Enroll OK' if row['hostname'] == row['Hostname'] else 'Máquina sem enroll', axis=1)
    
    # Cria coluna 'User' com base na comparação entre 'User' e 'email'. Se forem iguais, define como 'OK', senão mantém o valor original em 'User'
    df_merge_netskope['Email Enroll Netskope'] = df_merge_netskope.apply(lambda row: 'OK' if row['User'] == row.get('email', row['User']) else row['User'], axis=1)
    
    # Remover a coluna 'Hostname' e 'User'
    df_merge_netskope.drop(columns=['Hostname', 'User'], inplace=True)
    
    return df_merge_netskope


def crowstrike():
    df_crowdstrike = pd.read_csv(base_crowdstrike)
    dfbaseJC_filtered = jc_activated()
    
    # Merge com o CrowdStrike
    df_merge_crowdstrike = dfbaseJC_filtered.merge(df_crowdstrike[['Hostname']], left_on='hostname', right_on='Hostname', how='left')
    
    # Cria coluna 'Status Crowdstrike' # Valor baseado na correspondência do hostname
    df_merge_crowdstrike['Status Crowdstrike'] = df_merge_crowdstrike['Hostname'].apply(lambda x: 'Instalado' if pd.notna(x) else 'Não instalado')

    return df_merge_crowdstrike

def export():
    df_netskope_result = netskope()
    df_crowdstrike_result = crowstrike()

    # merge netskope e crowdstrike
    df_result = df_netskope_result.merge(df_crowdstrike_result[['hostname', 'Status Crowdstrike']], left_on='hostname', right_on='hostname', how='inner')
    
    df_result.to_csv('export.csv', index=False)


export()
