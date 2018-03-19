import json
import re
import sys
from utils import analyse
from aws import list_objects, get_object, put_object

bucket = sys.argv[1]

objects = list_objects(
        bucket=bucket,
        prefix='inbound/text-analysis/',
        delimiter='/'
        )['Contents']

regex = re.compile(r'\/$')
objects = list(filter(lambda x: not regex.search(x['Key']), objects))

for item in objects:
    key = item['Key']
    item_data = get_object(
            bucket=bucket,
            key=key
            )

    text = item_data['Body'].read()
    data = analyse(text)

    parts = key.split('/')
    newKey = 'sandbox/' + parts[len(parts) -1 ].split('.')[0] + '.json'
    print(newKey)
    put_object(
            bucket=bucket,
            key=newKey,
            body=data
            )
