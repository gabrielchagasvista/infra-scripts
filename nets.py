import pandas as pd

def jc_activated(): 
    dfbaseJC = pd.read_csv('base_jc2.csv')
    return dfbaseJC

def crowdstrike_netskope():
    df_netskope = pd.read_csv('base_netskope.csv')
    dfbaseJC_filtered = jc_activated()
    
    df_crowdstrike_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    df_crowdstrike_netskope['Status Enroll'] = df_crowdstrike_netskope.apply(lambda row: 'Enroll OK' if row['User'] == row['User'] else 'Máquina sem enroll', axis=1)
    df_crowdstrike_netskope['User'] = df_crowdstrike_netskope.apply(lambda row: 'Mesmo do jump' if row['User'] == row.get('email', row['User']) else 'Divergente', axis=1)
    
    df_crowdstrike_netskope.drop(columns=['Hostname'], inplace=True)  # Remover a coluna 'Hostname'
    
    # Renomear a coluna 'User' para 'Email Enroll Netskope'
    df_crowdstrike_netskope.rename(columns={'User': 'Email Enroll Netskope'}, inplace=True)
    
    # Reordenar as colunas no DataFrame resultante
    columns_order = list(dfbaseJC_filtered.columns) + ['Status Enroll', 'Email Enroll Netskope']
    df_result = df_crowdstrike_netskope[columns_order]
    
    df_result.to_csv('potoco.csv', index=False)

crowdstrike_netskope()
