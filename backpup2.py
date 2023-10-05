import pandas as pd

def jc_activated(): 
    dfbaseJC = pd.read_csv('base_jc2.csv')
    # dfbaseJC = pd.read_csv('prod-basejump.csv')
    return dfbaseJC

def crowdstrike_netskope():
    df_netskope = pd.read_csv('base_netskope.csv')
    df_crowdstrike = pd.read_csv('base_crowdstrike.csv')
    dfbaseJC_filtered = jc_activated()
    
    # merge netskope
    #df_crowdstrike_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    df_merge_netskope = dfbaseJC_filtered.merge(df_netskope[['Hostname', 'User']], left_on=['hostname'], right_on=['Hostname'], how='left')
    
    
    # merge Crowdstrike
    df_merge_crowdstrike = dfbaseJC_filtered.merge(df_crowdstrike[['Hostname']], left_on='hostname', right_on='Hostname', how='inner')
    
    
    # Junte os dois DataFrames usando o método concat
    df_combined = pd.concat([df_merge_netskope, df_merge_crowdstrike], axis=1)

    # Salvar o resultado em um arquivo CSV chamado 'potoco.csv'
    df_combined.to_csv('potoco.csv', index=False)
    
    # Cria coluna 'Status Enroll Netskope' baseado na correspondencia do hostname
    #df_crowdstrike_netskope['Status Enroll Netskope'] = df_crowdstrike_netskope.apply(lambda row: 'Enroll OK' if row['hostname'] == row['Hostname'] else 'Máquina sem enroll', axis=1)
        
    # Criar a coluna 'User' com base na comparação entre 'User' e 'email'. Se forem iguais, define como 'OK', senão mantém o valor original em 'User'
    #df_crowdstrike_netskope['Email Enroll Netskope'] = df_crowdstrike_netskope.apply(lambda row: 'OK' if row['User'] == row.get('email', row['User']) else row['User'], axis=1)
    
    # Remover a coluna 'Hostname' do DataFrame
    #df_crowdstrike_netskope.drop(columns=['Hostname', 'User'], inplace=True)
    
    # Definir a ordem das colunas no DataFrame resultante
    #columns_order = list(dfbaseJC_filtered.columns) + ['Status Enroll Netskope', 'Email Enroll Netskope'] 
    
    # Criar o DataFrame final com as colunas na ordem desejada
    #df_result = df_crowdstrike_netskope[columns_order]
    
    # Salvar o DataFrame final em um arquivo CSV chamado 'potoco.csv'
    #df_result.to_csv('potoco.csv', index=False)
    #df_crowdstrike_netskope.to_csv('potoco.csv', index=False)

# Chamar a função principal crowdstrike_netskope para realizar a análise
crowdstrike_netskope()
