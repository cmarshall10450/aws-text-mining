import json
import sys
from utils import flatten
from aws import get_batch_sentiment, get_batch_entities, get_batch_key_phrases, get_overall_sentiment


def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]


file = open(sys.argv[1], 'r')

text = file.read()

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

key_phrases = [get_batch_key_phrases(text_list=result[i])['ResultList']
               for i in range(0, len(result))]
key_phrases = flatten(key_phrases)
key_phrases = flatten([item['KeyPhrases'] for item in key_phrases])


for i in range(0, len(sentiments)):
    sentiments[i]['Index'] = i

data = {}

data['OverallSentimentScores'], data['OverallSentiment'] = get_overall_sentiment(
    sentiments)
data['Sentiments'] = sentiments
data['Entities'] = entities
data['KeyPhrases'] = key_phrases

with open(sys.argv[2], 'w') as outfile:
    json.dump(data, outfile, sort_keys=True, indent=2)
