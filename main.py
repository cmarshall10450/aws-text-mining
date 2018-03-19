import json
import re
import sys
from utils import analyse
from aws import list_objects, get_object, put_object

bucket = sys.argv[1]

# file = open(sys.argv[1], 'r')

# text = file.read()

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

    newKey = key.split('.')[0] + '.json'
    print(newKey)
    put_object(
            bucket=bucket,
            key=newKey,
            body=data
            )
# with open(sys.argv[2], 'w') as outfile:
    # json.dump(data, outfile, sort_keys=True, indent=2)
