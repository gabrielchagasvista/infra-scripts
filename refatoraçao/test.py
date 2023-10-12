import pandas as pd


df_basejc = pd.read_csv('base_jc.csv')

dfbaseJC = dfbaseJC[(dfbaseJC['user_state'] == 'ACTIVATED') & 
                    ~(dfbaseJC['username'].astype(str).str.startswith('userloftmac.local') | dfbaseJC['username'].astype(str).str.startswith('admgloft'))]

dfbaseJC = dfbaseJC[['email', 'hostname', 'device_os', 'resource_object_id']]


