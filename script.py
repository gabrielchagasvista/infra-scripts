import pandas as pd


def jc_activated(): #filtra apenas os usuários que estão ativos
    dfbaseJC = pd.read_csv('base_jc.csv')
    dfbaseJC['username'] = dfbaseJC['username'].astype(str)
    dfbaseJC_filtered = dfbaseJC[dfbaseJC['user_state'] == 'ACTIVATED']
    dfbaseJC_filtered.to_csv('jc_ACTIVATED.csv', index=False)
    return dfbaseJC_filtered

def crowdstrike(dfbaseJC_filtered):

    df_crowdstrike = pd.read_csv('base_crowdstrike.csv')
    #dfbase_crowdstrike = pd.read_csv('base_crowdstrike.csv')
    
    merged_df = df_filtered.merge(df_crowdstrike[['Serial Number']], left_on='serial_number', right_on='Serial Number', how='inner')
    merged_df['crowdstrike'] = merged_df['Serial Number'].apply(lambda x: 'instalado' if pd.notna(x) else 'nao instalado')
    
    # Seleciona apenas as colunas 'email', 'hostname' e 'device_os'
    merged_df = merged_df[['email', 'hostname', 'device_os', 'crowdstrike']]
    
    # Exporta as correspondências para o arquivo 'correspondencias.csv'
    merged_df.to_csv('correspondencias.csv', index=False)

# Chama a função jc_activated() e passa o DataFrame resultante para a função crowdstrike()
df_filtered = jc_activated()
crowdstrike(df_filtered)