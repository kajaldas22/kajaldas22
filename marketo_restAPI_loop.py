import pendulum
import requests
import base64
import logging
import json
import os
import pendulum
import requests
import sys
import tempfile
import time
import os
import pandas as pd
import boto3
import time
import sys
import time
from pytz import timezone

from datetime import datetime, timedelta
from marketo_activitytypeid_list import activitytypeids

client_id = 'client-id',
client_secret = 'client-secret',
host = 'https://999.mktorest.com'
email = 'daskajal@email.com'

region_name = 'us-east-1'
BUCKET = 'market
s3 = boto3.client('s3',
                  aws_access_key_id='xxxx',
                  aws_secret_access_key='xxxxx-xxxx',
                  region_name=region_name)

data = requests.get(host + '/identity/oauth/token',
                    params={'grant_type': 'client_credentials',
                            'client_id': client_id,
                            'client_secret': client_secret},
                    ).json()

from pprint import pprint

days_back = 1
start_date = pendulum.now('US/Pacific').subtract(days=days_back).strftime('%Y-%m-%d')
token = data['access_token']

data_nextPageToken = requests.get(host + '/rest/v1/activities/pagingtoken.json',
                                  params={'access_token': token,
                                          'sinceDatetime': start_date
                                          }
                                  ).json()

nextPageToken = data_nextPageToken['nextPageToken']
print(start_date)


def get_activity_type(nextPageToken, token):
    results = []
    tabname = 'activity_types'
    acttypes = requests.get(host + '/rest/v1/activities/types.json',
                            params={'access_token': token,
                                    'nextPageToken': nextPageToken}
                            ).text

    json_obj = json.loads(acttypes)
    print("api response:", json_obj['success'])
    results.extend(json_obj["result"])
    pprint(json_obj)
    write_to_s3(results, tabname, BUCKET, s3)


#  get activities data --------------
def get_activities(activity_id, token, nextPageToken):
    tabname = 'activities'
    results = []
    act1 = requests.get(host + '/rest/v1/activities.json',
                        params={'access_token': token,
                                'nextPageToken': nextPageToken,
                                'activityTypeIds': activity_id
                                })

    # json_obj = json.loads(act1)
    json_obj = act1.json()
    # nextPageToken = json_obj['nextPageToken']
    more_result = json_obj['moreResult']
    if not "result" in json_obj and not more_result:
        print("blank result", str(json_obj))
        #raise RuntimeError
        exit(0)

    results.extend(json_obj["result"])
    more_result = json_obj['moreResult']
    while True:
        act1 = requests.get(host + '/rest/v1/activities.json',
                            params={'access_token': token,
                                    'nextPageToken': nextPageToken,
                                    'activityTypeIds': activity_id
                                    }).text
        json_obj = json.loads(act1)
        results.extend(json_obj["result"])
        more_result = json_obj['moreResult']
        nextPageToken = json_obj['nextPageToken']
        # pprint(json_obj["result"])

        if not more_result:
            break

    # -- write to S3
    write_to_s3(results, tabname, BUCKET, s3)


def write_to_s3(results, tabname, BUCKET, s3):
    utc_now = pendulum.now('UTC')
    utc_now_date = utc_now.strftime('%Y%m%d')
    file_output_name = utc_now_date + "_" + str(int(time.time())) + ".jsonl"
    # print(f"Local temp output path: {local_restcopy_file_path}")
    with open(file_output_name, 'w') as f:
        for entry in results:
            json.dump(entry, f)
            f.write('\n')

    s3_path = tabname + "/" + file_output_name
    s3.upload_file(file_output_name, BUCKET, s3_path)
    os.remove(file_output_name)


# get_activity_type(nextPageToken, token)
r = [get_activities(id, token, nextPageToken) for id in activitytypeids]
