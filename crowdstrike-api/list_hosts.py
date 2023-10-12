#pip install crowdstrike-falconpy
from falconpy import Hosts
import pandas as pd

import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path('./.env')
load_dotenv(dotenv_path=env_path)

token = os.environ.get('CROWDSTRIKE')

def get_hosts():
    hosts = Hosts(client_id='d7215357871641e086c4edf1dee48d04',client_secret=token)

    hosts_search_result = hosts.query_devices_by_filter(limit=5000, offset=0, sort="hostname.asc")

    response_list = []
    values_dict = {}

    if hosts_search_result["status_code"] == 200:
        hosts_found = hosts_search_result["body"]["resources"]
        # Confirm our search produced results
        if hosts_found:
            # Retrieve the details for all matches
            hosts_detail = hosts.get_device_details(ids=hosts_found)["body"]["resources"]
            for detail in hosts_detail:
                #print(detail)
                for key, value in detail.items():
                    if isinstance(value, str) or isinstance(value, int):
                        values_dict[key] = value
                    elif isinstance(value, dict):
                        if key == 'device_policies':
                            for new_key, info in value.items():
                                for value_key, new_value in info.items():
                                    new2_key = f"{key}_{new_key}_{value_key}"
                                    #print(f"{new_key}: {new_value}")
                                    values_dict[new2_key] = new_value
                    elif isinstance(value, list):
                        if key == 'policies':
                            for info in value:
                                for new_key, new_value in info.items():
                                    new2_key = f"{key}_{new_key}"
                                    #print(f"{new2_key}: {new_value}")
                                    values_dict[new2_key] = new_value
                        if key == 'groups':
                            #uma lista comum
                            #['cdf910dd6b9242a3ac0fff132747e88a', '5302f1e513ca4d2cb9a1d9ccca9f2e08', 'a52b73a736d74b37bc5b375015d95c25']
                            values_dict[key] = value
                    else:
                        pass

                response_list.append(pd.DataFrame([values_dict]))

    result_df = pd.concat(response_list, ignore_index=True)
    #result_df.to_csv('all_hosts.csv', sep=';', encoding='UTF-8', index=False)
    return result_df

if __name__ == '__main__':
    df = get_hosts()
    df.to_csv('all_hosts.csv', sep=';', encoding='UTF-8', index=False)