import pandas as pd
from datetime import datetime
from sheets import read_sheets

id_base_jc='1jTEL86mE80AjDCoZ6C63MOe4-jK5b9_vezdpVewxdGo'
id_lastcontact='1cN5QU1ICdIadoI5-uWL6T_WB5AHFt3x38NJVWuGlCgw'
id_base_netskope='1a5XTXL5_yeuj0Eq_4sR2uNkjIF_2IqHnTpS4qMmudJ8'
id_base_crowdstrike='1LdUfDJj16k6cEmtEGkw5Ss3tNwGu0YXTSemv3Kg1b8M'

def jc_activated():
    dfbaseJC = read_sheets(id_base_jc)
    jc_lastcontact = read_sheets(id_lastcontact)[['_id', 'lastContact']]

    dfbaseJC['hostname'].replace('', pd.NA, inplace=True)
    dfbaseJC.dropna(subset=['hostname'], inplace=True)
    
    dfbaseJC = dfbaseJC[(dfbaseJC['user_state'] == 'ACTIVATED') & 
                        ~dfbaseJC['username'].astype(str).str.startswith(('userloftmac.local', 'admgloft'))]

    dfbaseJC_consolidado = dfbaseJC.merge(jc_lastcontact, left_on='resource_object_id', right_on='_id', how='left')
    
    dfbaseJC_consolidado['lastContact'] = pd.to_datetime(dfbaseJC_consolidado['lastContact'].str.split('T').str[0], format="%Y-%m-%d")
    
    # Calcular os dias sem contato
    dfbaseJC_consolidado['dias sem contato'] = (datetime.today() - dfbaseJC_consolidado['lastContact']).dt.days.fillna(0).astype(int)
    
    # Formatar 'lastContact' para o formato desejado
    dfbaseJC_consolidado['lastContact'] = dfbaseJC_consolidado['lastContact'].dt.strftime("%d-%m-%Y")

    return dfbaseJC_consolidado

def netskope():
    df_netskope = read_sheets(id_base_netskope)
    dfbaseJC_filtered = jc_activated()
    
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope, left_on=['hostname'], right_on=['Hostname'], how='left')
    
    df_merge_netskope['Status Enroll Netskope'] = df_merge_netskope.apply(lambda row: 
    'Máquina sem enroll' if pd.isna(row['Hostname']) else
    'Enroll OK' if (row['User'] == row['email']) else
    'Verificar enroll', axis=1)

    group_name = "GRP-BR-NETSKOPE"

    df_merge_netskope['Install Netskope'] = ''

    df_merge_netskope.loc[df_merge_netskope['Status Enroll Netskope'] == 'Máquina sem enroll', 'Install Netskope'] = df_merge_netskope['resource_object_id'].apply(
        lambda system_id: f"Add-JCSystemGroupMember -GroupName {group_name} -SystemID {system_id}"
    )

    df_merge_netskope.loc[df_merge_netskope['Status Enroll Netskope'] == 'Verificar enroll', 'Install Netskope'] = df_merge_netskope['resource_object_id'].apply(
        lambda system_id: f"Add-JCSystemGroupMember -GroupName {group_name} -SystemID {system_id}"
    )
    
    return df_merge_netskope


def crowdstrike():
    
    df_crowdstrike = read_sheets(id_base_crowdstrike)[['Serial Number']]
    dfbaseJC_filtered = jc_activated()

    df_merge_crowdstrike = pd.merge(dfbaseJC_filtered, df_crowdstrike, left_on='serial_number', right_on='Serial Number', how='left')
    
    df_merge_crowdstrike['Status Crowdstrike'] = df_merge_crowdstrike['Serial Number'].apply(lambda x: 
        'Instalado' if pd.notna(x) else 'Não instalado')
    
    group_name = "GRP-BR-CROWDSTRIKE"
    
    df_merge_crowdstrike['Install Crowdstrike'] = ''
    
    df_merge_crowdstrike.loc[df_merge_crowdstrike['Status Crowdstrike'] == 'Não instalado', 'Install Crowdstrike'] = df_merge_crowdstrike['resource_object_id'].apply(
        lambda system_id: f"Add-JCSystemGroupMember -GroupName {group_name} -SystemID {system_id}"
    )
    
    return df_merge_crowdstrike

def export():
    
    df_netskope_result = netskope()
    df_crowdstrike_result = crowdstrike()
    
    df_result = pd.merge(df_netskope_result, df_crowdstrike_result, how='outer')
    
    #colunas resultado final
    df_result = df_result[['email', 'hostname', 'device_os', 'lastContact', 'dias sem contato', 'Status Enroll Netskope', 'Install Netskope', 'Status Crowdstrike', 'Install Crowdstrike']]


    df_result.to_csv('controle_ns_cs.csv', index=False)

export()
