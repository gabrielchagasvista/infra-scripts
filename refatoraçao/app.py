import pandas as pd
from datetime import datetime

base_jc = 'csv-prod/base_jc.csv'
last_contact = 'csv-prod/jc_lastcontact.csv'
base_netskope = 'csv-prod/base_netskope.csv'
base_crowdstrike = 'csv-prod/base_crowdstrike.csv'

#base_jc = 'csv-dev/base_jc2.csv'
#base_netskope = 'csv-dev/base_netskope.csv'
#base_crowdstrike = 'csv-dev/base_crowdstrike.csv'


def jc_filtered():
    dfbaseJC = pd.read_csv(base_jc)
    df



def jc_activated(): 
    dfbaseJC = pd.read_csv(base_jc)
    jc_lastcontact = pd.read_csv(last_contact, usecols=['_id', 'lastContact'])

    dfbaseJC = dfbaseJC[(dfbaseJC['user_state'] == 'ACTIVATED') & 
                        ~(dfbaseJC['username'].astype(str).str.startswith('userloftmac.local') | 
                          dfbaseJC['username'].astype(str).str.startswith('admgloft'))
                       ]
    dfbaseJC = dfbaseJC[['email', 'hostname', 'device_os', 'resource_object_id']]


    dfbaseJC_consolidado = pd.merge(dfbaseJC, jc_lastcontact, left_on='resource_object_id', right_on='_id', how='left')
    
    dfbaseJC_consolidado.drop(columns=['_id', 'resource_object_id'], inplace=True)

    # transforma lastContact em DD-MM-YYYY
    dfbaseJC_consolidado['lastContact'] = dfbaseJC_consolidado['lastContact'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d-%m-%Y") if pd.notna(x) else None)
    
    return dfbaseJC_consolidado


def netskope():
    df_netskope = pd.read_csv(base_netskope)
    dfbaseJC_filtered = jc_activated()
    
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    df_merge_netskope['Status Enroll Netskope'] = df_merge_netskope.apply(lambda row: 
        'Enroll OK' if (row['hostname'] == row['Hostname'] and row['User'] == row['email']) 
        else ('Verificar enroll' if (row['hostname'] == row['Hostname'] and row['User'] != row['email'])
        else 'Máquina sem enroll'), axis=1)
    
    df_merge_netskope.drop(columns=['Hostname', 'User'], inplace=True)
    
    return df_merge_netskope


def crowstrike():
    df_crowdstrike = pd.read_csv(base_crowdstrike)
    dfbaseJC_filtered = jc_activated()
    
    df_merge_crowdstrike = dfbaseJC_filtered.merge(df_crowdstrike[['Hostname']], left_on='hostname', right_on='Hostname', how='left')
    
    df_merge_crowdstrike['Status Crowdstrike'] = df_merge_crowdstrike['Hostname'].apply(lambda x: 
        'Instalado' if pd.notna(x) else 'Não instalado')

    df_merge_crowdstrike.drop(columns=['Hostname'], inplace=True)

    return df_merge_crowdstrike

def export():
    df_netskope_result = netskope()
    df_crowdstrike_result = crowstrike()
    
    df_result = df_netskope_result.merge(df_crowdstrike_result, how='outer')
    
    df_result.to_csv('export.csv', index=False)


export()
