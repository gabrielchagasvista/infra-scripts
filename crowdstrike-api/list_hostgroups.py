#pip install crowdstrike-falconpy
from falconpy import HostGroup
import pandas as pd

import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path('./.env')
load_dotenv(dotenv_path=env_path)

token = os.environ.get('CROWDSTRIKE')

hosts_group = HostGroup(client_id='d7215357871641e086c4edf1dee48d04',client_secret=token)

hosts_group_search_result = hosts_group.query_host_groups(limit=50, offset=0)

response_list = []
values_dict = {}

if hosts_group_search_result["status_code"] == 200:
    hosts_found = hosts_group_search_result["body"]["resources"]
    # Confirm our search produced results
    if hosts_found:
        hosts_detail = hosts_group.getHostGroups(ids=hosts_found)["body"]["resources"]
        for detail in hosts_detail:
            #print(detail)
            for key, value in detail.items():
                if isinstance(value, str) or isinstance(value, int):
                    values_dict[key] = value
                elif isinstance(value, dict):
                    pass
                elif isinstance(value, list):
                    pass
                else:
                    pass
            response_list.append(pd.DataFrame([values_dict]))

result_df = pd.concat(response_list, ignore_index=True)
result_df.to_csv('all_hosts_group_info.csv', sep=',', encoding='UTF-8', index=False)