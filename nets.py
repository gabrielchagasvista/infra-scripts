import pandas as pd
from datetime import datetime
import sheets

base_jc = 'csv-prod/base_jc.csv'
last_contact = 'csv-prod/jc_lastcontact.csv'
base_netskope = 'csv-prod/base_netskope.csv'
base_crowdstrike = 'csv-prod/base_crowdstrike.csv'

#base_jc = 'csv-dev/base_jc2.csv'
#last_contact = 'csv-dev/jc_lastcontact.csv'
#base_netskope = 'csv-dev/base_netskope.csv'
#base_crowdstrike = 'csv-dev/base_crowdstrike.csv'

def jc_activated():
    dfbaseJC = pd.read_csv(base_jc)
    jc_lastcontact = pd.read_csv(last_contact, usecols=['_id', 'lastContact'])

    dfbaseJC = dfbaseJC[(dfbaseJC['user_state'] == 'ACTIVATED') & 
                        ~dfbaseJC['username'].astype(str).str.startswith(('userloftmac.local', 'admgloft'))]

    dfbaseJC_consolidado = dfbaseJC.merge(jc_lastcontact, left_on='resource_object_id', right_on='_id', how='left')
    
    dfbaseJC_consolidado['lastContact'] = pd.to_datetime(dfbaseJC_consolidado['lastContact'], format="%Y-%m-%dT%H:%M:%S.%fZ").dt.strftime("%d-%m-%Y")
    
    return dfbaseJC_consolidado


def netskope():
    df_netskope = pd.read_csv(base_netskope)
    dfbaseJC_filtered = jc_activated()
    
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    df_merge_netskope['Status Enroll Netskope'] = df_merge_netskope.apply(lambda row: 
        'Enroll OK' if (row['hostname'] == row['Hostname'] and row['User'] == row['email']) 
        else ('Verificar enroll' if (row['hostname'] == row['Hostname'] and row['User'] != row['email'])
        else 'Máquina sem enroll'), axis=1)
    
    group_name = "GRP-BR-NETSKOPE"
    df_merge_netskope['Install Nestkope'] = df_merge_netskope.apply(lambda row: 
        f"Add-JCSystemGroupMember -GroupName {group_name} -SystemID {row['resource_object_id']}"
        if row['Status Enroll Netskope'] == 'Máquina sem enroll'
        else '', axis=1)
    
    return df_merge_netskope


def crowstrike():
    df_crowdstrike = pd.read_csv(base_crowdstrike, usecols=['Serial Number'])
    dfbaseJC_filtered = jc_activated()

    df_merge_crowdstrike = pd.merge(dfbaseJC_filtered, df_crowdstrike, left_on='serial_number', right_on='Serial Number', how='left')
    
    df_merge_crowdstrike['Status Crowdstrike'] = df_merge_crowdstrike['Serial Number'].apply(lambda x: 
        'Instalado' if pd.notna(x) else 'Não instalado')
    
    group_name = "GRP-BR-CROWDSTRIKE"
    df_merge_crowdstrike['Install Crowdstrike'] = df_merge_crowdstrike.apply(lambda row: 
        f"Add-JCSystemGroupMember -GroupName {group_name} -SystemID {row['resource_object_id']}"
        if row['Status Crowdstrike'] == 'Não instalado'
        else '', axis=1)
    
    return df_merge_crowdstrike

def export():
    
    df_netskope_result = netskope()
    df_crowdstrike_result = crowstrike()
    
    df_result = pd.merge(df_netskope_result, df_crowdstrike_result, how='outer')
    
    #colunas que serão mostradas no final
    df_result = df_result[['email', 'hostname', 'device_os', 'lastContact', 'Status Enroll Netskope','Install Nestkope','Status Crowdstrike', 'Install Crowdstrike']]
    
    df_result.to_csv('export.csv', index=False)


export()