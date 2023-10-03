import pandas as pd

def jc_activated(): 
    dfbaseJC = pd.read_csv('base_jc2.csv')
    return dfbaseJC

def crowdstrike_netskope():
    df_netskope = pd.read_csv('base_netskope.csv')
    dfbaseJC_filtered = jc_activated()
    
    df_crowdstrike_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    df_crowdstrike_netskope['Hostname'].fillna('Não encontrado', inplace=True)
    df_crowdstrike_netskope['Hostname'] = df_crowdstrike_netskope.apply(lambda row: 'Enroll OK' if row['Hostname'] != 'Máquina sem enroll' else 'Máquina sem enroll', axis=1)
    df_crowdstrike_netskope.rename(columns={'Hostname': 'Status enroll'}, inplace=True)
    df_crowdstrike_netskope['User'] = df_crowdstrike_netskope.apply(lambda row: 'OK' if row['User'] == row['email'] else 'Divergente base JC', axis=1)
    
    df_crowdstrike_netskope.rename(columns={'User': 'Email Enroll Netskope'}, inplace=True)
    
    df_crowdstrike_netskope.to_csv('potoco.csv', index=False)

# Chamar a função crowdstrike_netskope() para realizar a mesclagem e criar o arquivo correspondencias.csv
crowdstrike_netskope()
