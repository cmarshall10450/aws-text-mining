from aws import *


def flatten(array):
    return [item for sublist in array for item in sublist]


def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]


def analyse(text):
    chunk_size = 5000
    text_chunks = chunks(text, chunk_size)

    max_result_length = 25
    result = chunks(text_chunks, max_result_length)

    sentiments = [get_batch_sentiment(text_list=result[i])['ResultList']
                  for i in range(0, len(result))]
    sentiments = flatten(sentiments)

    entities = [get_batch_entities(text_list=result[i])['ResultList']
                for i in range(0, len(result))]
    entities = flatten(entities)
    entities = flatten([item['Entities'] for item in entities])

    entity_count = {}
    for entity in entities:
        if entity['Text'] in entity_count.keys():
            entity_count[entity['Text']] += 1
        else:
            entity_count[entity['Text']] = 1

    key_phrases = [get_batch_key_phrases(text_list=result[i])['ResultList']
                   for i in range(0, len(result))]
    key_phrases = flatten(key_phrases)
    key_phrases = flatten([item['KeyPhrases'] for item in key_phrases])

    key_phrase_count = {}
    for key_phrase in key_phrases:
        if key_phrase['Text'] in key_phrase_count.keys():
            key_phrase_count[key_phrase['Text']] += 1
        else:
            key_phrase_count[key_phrase['Text']] = 1

    for i in range(0, len(sentiments)):
        sentiments[i]['Index'] = i

    data = {}

    data['OverallSentimentScores'], data['OverallSentiment'] = get_overall_sentiment(
        sentiments)
    data['Sentiments'] = sentiments
    data['Entities'] = entities
    data['EntityCount'] = entity_count
    data['KeyPhrases'] = key_phrases
    data['KeyPhraseCount'] = key_phrase_count

    return data
