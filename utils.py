from aws import *


def flatten(array):
    return [item for sublist in array for item in sublist]


def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]


def get_analysed_tags(bucket):
    analysed_tags_object = get_object(
        bucket=bucket,
        key='sandbox/text-analysis/analysed-files.json'
        )['Body'].read()

    analysed_tags = json.loads(analysed_tags_object)['Tags']

    return analysed_tags


def get_chunks(text, chunk_size, max_chunk_count):
    text_chunks = chunks(text, chunk_size)
    result = chunks(text_chunks, max_chunk_count)

    return result


def get_sentiments(text_list):
    sentiments = [get_batch_sentiment(text_list=text_list[i])['ResultList']
                  for i in range(0, len(text_list))]
    sentiments = flatten(sentiments)
    for i in range(0, len(sentiments)):
        sentiments[i]['Index'] = i

    return sentiments


def get_entities(text_list):
    entities = [get_batch_entities(text_list=text_list[i])['ResultList']
                for i in range(0, len(text_list))]
    entities = flatten(entities)
    entities = flatten([item['Entities'] for item in entities])

    return entities


def get_entity_count(entities):
    entity_count = {}
    for entity in entities:
        if entity['Text'] in entity_count.keys():
            entity_count[entity['Text']] += 1
        else:
            entity_count[entity['Text']] = 1

    return entity_count


def get_key_phrases(text_list):
    key_phrases = [get_batch_key_phrases(text_list=text_list[i])['ResultList']
                   for i in range(0, len(text_list))]
    key_phrases = flatten(key_phrases)
    key_phrases = flatten([item['KeyPhrases'] for item in key_phrases])

    return key_phrases

def get_key_phrase_count(key_phrases):
    key_phrase_count = {}
    for key_phrase in key_phrases:
        if key_phrase['Text'] in key_phrase_count.keys():
            key_phrase_count[key_phrase['Text']] += 1
        else:
            key_phrase_count[key_phrase['Text']] = 1

    return key_phrase_count

def analyse_object(item, bucket, outputPrefix):
    key = item['Key']

    print('\nAnalysing: ' + key)

    item_data = get_object(
        bucket=bucket,
        key=key
        )

    text = item_data['Body'].read()
    data = analyse(text)

    parts = key.split('/')
    newKey = outputPrefix + \
        parts[len(parts) - 1].split('.')[0] + '.json'

    return [newKey, data]


def analyse(text):
    chunk_size = 5000
    max_chunk_count = 25
    text_list = get_chunks(text, chunk_size, max_chunk_count)

    sentiments = get_sentiments(text_list)

    entities = get_entities(text_list)
    entity_count = get_entity_count(entities)

    key_phrases = get_key_phrases(text_list)
    key_phrase_count = get_key_phrase_count(key_phrases)

    data = {}
    data['OverallSentimentScores'], data['OverallSentiment'] = get_overall_sentiment(
        sentiments)
    data['Sentiments'] = sentiments
    data['Entities'] = entities
    data['EntityCount'] = entity_count
    data['KeyPhrases'] = key_phrases
    data['KeyPhraseCount'] = key_phrase_count

    return data

def save_etags(bucket, key, objects):
    tags = {}
    tags['Tags'] = [obj['ETag'] for obj in objects]

    put_object(
            bucket=bucket,
            key=key,
            body=tags
            )
