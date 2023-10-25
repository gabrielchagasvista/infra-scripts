import pandas as pd
from sheets import read_sheets


id_base_jc='1jTEL86mE80AjDCoZ6C63MOe4-jK5b9_vezdpVewxdGo'
id_device_models='1iA-8JnSH4T26cOZAy7MPE5Xe7yYKzYkCoT2XEk21a7k'


def pc_models():  
    dfbaseJC = read_sheets(id_base_jc)[['resource_object_id', 'device_os', 'user_state']]
    dfdevice_models = read_sheets(id_device_models)

    dfpcmodels_os = dfdevice_models.merge(dfbaseJC, left_on=['SystemId'], right_on=['resource_object_id'], how='left')

    dfpcmodels_os = dfpcmodels_os[['ComputerName','user_state', 'HardwareVendor', 'HardwareModel', 'device_os']]
    return dfpcmodels_os

def groups():
    
    dfpcmodels_os = pc_models()
    
    #GRP-BR-Windows-Dell --- global (Loft, CP, Vista)
    grpbr_windows_dell = dfpcmodels_os.loc[dfpcmodels_os['HardwareVendor'] == 'Dell Inc.'].copy()
    grpbr_windows_dell['Device Group'] = 'GRP-BR-Windows-Dell'

    #GRP-BR-Windows --- somente loft
    grpbr_windows = dfpcmodels_os.loc[dfpcmodels_os['ComputerName'].str.startswith('LOFT-') & (dfpcmodels_os['device_os'] == 'Windows')].copy()
    grpbr_windows['Device Group'] = 'GRP-BR-Windows'

    #GRP-BR-Linux --- somente loft  
    grpbr_linux = dfpcmodels_os.loc[dfpcmodels_os['ComputerName'].str.startswith('LOFT-') & dfpcmodels_os['device_os'].isin(['Ubuntu', 'LinuxMint', 'Debian'])].copy()
    grpbr_linux['Device Group'] = 'GRP-BR-Linux'

    #GRP-BR-MDM-MAC --- global
    grpbr_mac = dfpcmodels_os.loc[dfpcmodels_os['device_os'] == 'macOS'].copy()
    grpbr_mac['Device Group'] = 'GRP-BR-MDM-MAC'

    ##VS-GRP-Devices-Win --- somente vista
    vsgrp_windows = dfpcmodels_os.loc[(dfpcmodels_os['ComputerName'].str.startswith('VISTA-')) & (dfpcmodels_os['device_os'] == 'Windows')].copy()
    vsgrp_windows['Device Group'] = 'VS-GRP-Devices-Win'
    #vsgrp_windows.to_csv('linx.csv', index=False)

    #CP-GRP-Devices-Windows --- somente credpago
    cpgrp_windows = dfpcmodels_os.loc[dfpcmodels_os['ComputerName'].str.startswith('CP-') & (dfpcmodels_os['device_os'] == 'Windows')].copy()
    cpgrp_windows['Device Group'] = 'CP-GRP-Devices-Windows'
    
    #CP-GRP-Devices-Linux --- somente credpago
    cpgrp_linux = dfpcmodels_os.loc[dfpcmodels_os['ComputerName'].str.startswith('CP-') & dfpcmodels_os['device_os'].isin(['Ubuntu', 'LinuxMint', 'Debian'])].copy()
    cpgrp_linux['Device Group'] = 'CP-GRP-Devices-Linux'
    
    all_groups = pd.concat([grpbr_windows_dell, grpbr_windows, grpbr_linux, grpbr_mac, vsgrp_windows, cpgrp_windows, cpgrp_linux], axis=0)
    all_groups.to_csv('linx.csv', index=False)

        
groups()