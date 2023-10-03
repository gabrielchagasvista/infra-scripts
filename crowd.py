import pandas as pd

import pandas as pd

def jc_activated(): 
    dfbaseJC = pd.read_csv('base_jc.csv')
    dfbaseJC['username'] = dfbaseJC['username'].astype(str)
    dfbaseJC_filtered = dfbaseJC[dfbaseJC['user_state'] == 'ACTIVATED']
    dfbaseJC_filtered.to_csv('jc_ACTIVATED.csv', index=False)
    return dfbaseJC_filtered

def crowdstrike_netskope():
    df_crowdstrike = pd.read_csv('base_crowdstrike.csv')
    dfbaseJC_filtered = jc_activated()
    
    merged_df = dfbaseJC_filtered.merge(df_crowdstrike[['Serial Number']], left_on='serial_number', right_on='Serial Number', how='inner')
    merged_df['crowdstrike'] = merged_df['Serial Number'].apply(lambda x: 'instalado' if pd.notna(x) else 'nao instalado')
    
    merged_df = merged_df[['email', 'hostname', 'device_os', 'crowdstrike']]
    
    merged_df.to_csv('correspondencias.csv', index=False)



