import json
import boto3
from botocore.exceptions import ClientError

comp = boto3.client(service_name='comprehend', region_name='eu-west-1')
s3 = boto3.client(service_name='s3', region_name='eu-west-1')


def get_batch_sentiment(text_list):
    try:
        sentiment = comp.batch_detect_sentiment(
            TextList=text_list,
            LanguageCode='en'
        )

        return sentiment
    except ClientError as e:
        return e.response['Error']


def get_batch_entities(text_list):
    try:
        entities = comp.batch_detect_entities(
            TextList=text_list,
            LanguageCode='en'
        )

        return entities
    except ClientError as e:
        return e.response['Error']


def get_batch_key_phrases(text_list):
    try:
        entities = comp.batch_detect_key_phrases(
            TextList=text_list,
            LanguageCode='en'
        )

        return entities
    except ClientError as e:
        return e.response['Error']

def get_overall_sentiment(sentiment_list):
    scores = [0, 0, 0, 0]
    sentiments = ['MIXED', 'NEUTRAL', 'POSITIVE', 'NEGATIVE']

    for sentiment in sentiment_list:
        scores[0] += sentiment['SentimentScore']['Mixed']
        scores[1] += sentiment['SentimentScore']['Neutral']
        scores[2] += sentiment['SentimentScore']['Positive']
        scores[3] += sentiment['SentimentScore']['Negative']

    scores[0] /= len(sentiment_list)
    scores[1] /= len(sentiment_list)
    scores[2] /= len(sentiment_list)
    scores[3] /= len(sentiment_list)

    sentiment_scores = {
        'Mixed': scores[0],
        'Neutral': scores[1],
        'Positive': scores[2],
        'Negative': scores[3]
    }

    overall_sentiment_index = scores.index(max(scores))
    overall_sentiment = sentiments[overall_sentiment_index]

    return [sentiment_scores, overall_sentiment]

def list_objects(bucket, prefix, delimiter):
    return s3.list_objects(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter
            )

def get_object(bucket, key):
    try:
        obj = s3.get_object(
                Bucket=bucket,
                Key=key
                )

        return obj
    except ClientError as e:
        return obj.response['Error']

def put_object(bucket, key, body):
    try:
        print(json.dumps(body, sort_keys=True, indent=2)
                )
        obj = s3.put_object(
                Bucket=bucket,
                Key=key,
                Body='b' + json.dumps(body, sort_keys=True, indent=2),
                )
        return obj
    except ClientError as e:
        return e.response['Error']
