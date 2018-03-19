import json
import sys
import re
import argparse
from utils import analyse
from aws import list_objects, get_object, put_object

parser = argparse.ArgumentParser()

parser.add_argument('--bucket', help='S3 bucket')
parser.add_argument('--inputPrefix', help='Input prefix')
parser.add_argument('--outputPrefix', help='Output prefix')
args = vars(parser.parse_args())

bucket = args['bucket']

objects = list_objects(
    bucket=bucket,
    prefix=args['inputPrefix'],
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
    newKey = args['outputPrefix'] + \
        parts[len(parts) - 1].split('.')[0] + '.json'
    print(newKey)
    put_object(
        bucket=bucket,
        key=newKey,
        body=data
    )
