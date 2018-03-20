import json
import sys
import re
import argparse
from utils import analyse_object, save_etags, get_analysed_tags
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

analysed_tags = get_analysed_tags(bucket)

save_etags(
        bucket=bucket,
        key='sandbox/text-analysis/analysed-files.json',
        objects=objects
        )

for item in objects:
    if not item['ETag'] in analysed_tags:
        newKey, data = analyse_object(item, bucket, args['outputPrefix'])
        print('Result: '+ newKey)

        put_object(
            bucket=bucket,
            key=newKey,
            body=data
        )
