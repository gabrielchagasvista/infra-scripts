import pandas as pd

def jc_activated(): 
    dfbaseJC = pd.read_csv('base_jc2.csv')
    dfbaseJC_filtered = dfbaseJC[dfbaseJC['user_state'] == 'ACTIVATED']
    dfbaseJC_filtered.to_csv('jc_ACTIVATED.csv', index=False)
    return dfbaseJC_filtered

def netskope():
    df_netskope = pd.read_csv('base_netskope.csv')
    dfbaseJC_filtered = jc_activated()
    
    # merge netskope
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    # Cria coluna 'Status Enroll Netskope' # Valor baseado na correspondência do hostname
    df_merge_netskope['Status Enroll Netskope'] = df_merge_netskope.apply(lambda row: 'Enroll OK' if row['hostname'] == row['Hostname'] else 'Máquina sem enroll', axis=1)
    
    # Criar a coluna 'User' com base na comparação entre 'User' e 'email'. Se forem iguais, define como 'OK', senão mantém o valor original em 'User'
    df_merge_netskope['Email Enroll Netskope'] = df_merge_netskope.apply(lambda row: 'OK' if row['User'] == row.get('email', row['User']) else row['User'], axis=1)
    
    # Remover a coluna 'Hostname' e 'User'
    df_merge_netskope.drop(columns=['Hostname', 'User'], inplace=True)
    
    return df_merge_netskope

def crowstrike():
    df_crowdstrike = pd.read_csv('base_crowdstrike.csv')
    dfbaseJC_filtered = jc_activated()
    
    # Merge com o CrowdStrike
    df_merge_crowdstrike = dfbaseJC_filtered.merge(df_crowdstrike[['Hostname']], left_on='hostname', right_on='Hostname', how='inner')
    
    # Cria coluna 'Status Crowdstrike' # Valor baseado na correspondência do hostname
    df_merge_crowdstrike['Status Crowdstrike'] = df_merge_crowdstrike['Hostname'].apply(lambda x: 'Instalado' if pd.notna(x) else 'Não instalado')

    return df_merge_crowdstrike

def export():
    df_netskope_result = netskope()
    df_crowdstrike_result = crowstrike()

    # Combinar os dois DataFrames usando a chave comum 'hostname'
    df_combined = df_netskope_result.merge(df_crowdstrike_result[['hostname', 'Status Crowdstrike']], left_on='hostname', right_on='hostname', how='inner')
    
    # Salvar o DataFrame final em um arquivo CSV chamado 'potoco.csv'
    df_combined.to_csv('potoco.csv', index=False)


export()
