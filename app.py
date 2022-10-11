import os 
import datetime
from sqs_listener import SqsListener
from model_worker import model_worker
import boto3
import json
import functools

QUEUE_NAME = 'tutorialMiningQueue'
AWS_REGION = 'us-east-1'
SENTIMENT_DATA_TABLE = 'tutorialSentimentData'

if "QUEUE_NAME" in os.environ:
    QUEUE_NAME = os.environ['QUEUE_NAME']

if "AWS_REGION" in os.environ:
    AWS_REGION = os.environ['AWS_REGION']

if "SENTIMENT_DATA_TABLE" in os.environ:
    SENTIMENT_DATA_TABLE = os.environ['SENTIMENT_DATA_TABLE']

session = boto3.session.Session()
s3 = session.resource('s3', region_name=AWS_REGION)
dynamodb = session.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(SENTIMENT_DATA_TABLE)

class DBConnectionError(Exception):
    """Raised when something goes wrong with DynamoDB connection handling"""
    pass

def write_to_db(output_data):
    try:
        # adding unique key to each document because dynamoDB doesn't
        print('attempting to connect to db...')
        result = table.put_item(Item=output_data, ReturnValues='ALL_OLD')
        print(result)
        if result['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise DBConnectionError
    except Exception as err:
        print('ERR insert failed:', err)

def process_message(body, attributes, message_attributes):
    try:
        message = json.loads(body['Message'])
    except:
        # already JSON, use as is
        message = body['Message']
    records = message['Records']
    for record in records:
        # defining all the coins we lookup, here
        s3_data = record['s3']
        bucket = s3_data['bucket']
        bucket_name = bucket['name']
        object_data = s3_data['object']
        object_key = object_data['key']
        # transform object_key back to a valid timestamp
        print(f'bucket_name: {bucket_name} object_key: {object_key}')
        start_time = datetime.datetime.now()
        # get data file from s3 bucket
        s3.Object(bucket_name,object_key).download_file(f'{object_key}')
        f = open(f'{object_key}')
        data_file = json.load(f)
        f.close()
        # done with file now delete it or else the container's
        os.remove(f'{object_key}')
        # do some cleanup
        bitcoin_posts_arr = []
        ethereum_posts_arr = []
        dogecoin_posts_arr = []
        chainlink_posts_arr = []
        polkadot_posts_arr = []

        bitcoin_mention_count = 0
        ethereum_mention_count = 0
        dogecoin_mention_count = 0
        chainlink_mention_count = 0
        polkadot_mention_count = 0

        timestamp = data_file['timestamp'] 

        for post in data_file['posts']:
            # run through model
            # Try "joining" title and content (if content) to one string array?
            content = post['content']
            title = post['title']
            post_text_content = (title + ' ' + content).strip().replace('\n', '')

            if any(x in post_text_content for x in ["Bitcoin" , "bitcoin" ,"BTC", "btc"]):
                bitcoin_posts_arr.append(post_text_content)
                bitcoin_mention_count += 1
            if any(x in post_text_content for x in ["Ethereum" , "ethereum" ,"ETH", "eth", "ether"]):
                ethereum_posts_arr.append(post_text_content)
                ethereum_mention_count += 1
            if any(x in post_text_content for x in ["Dogecoin" , "dogecoin" ,"DOGE", "doge"]):
                dogecoin_posts_arr.append(post_text_content)
                dogecoin_mention_count += 1
            if any(x in post_text_content for x in ["Chainlink" , "chainlink" ,"LINK"]):
                chainlink_posts_arr.append(post_text_content)
                chainlink_mention_count += 1
            if any(x in post_text_content for x in ["Polkadot" , "polkadot","DOT", "dot"]):
                polkadot_posts_arr.append(post_text_content)
                polkadot_mention_count += 1

        if (len(bitcoin_posts_arr) > 0):
            bitcoin_results = model_worker(bitcoin_posts_arr)
        else:
            bitcoin_results = [-1]
        if (len(ethereum_posts_arr) > 0):
            ethereum_results = model_worker(ethereum_posts_arr)
        else:
            ethereum_results = [-1]
        if (len(dogecoin_posts_arr) > 0):
            dogecoin_results = model_worker(dogecoin_posts_arr)
        else:
            dogecoin_results = [-1]
        if (len(chainlink_posts_arr) > 0):
            chainlink_results = model_worker(chainlink_posts_arr)
        else:
            chainlink_results = [-1]
        if (len(polkadot_posts_arr) > 0):
            polkadot_results = model_worker(polkadot_posts_arr)
        else:
            polkadot_results = [-1]

        # sum these results into a single "score" for this timestamp to graph while preserving both post frequency (as a magnitude) 
        # overall sentiment (higher the score, the more positive, since positive = 1 and negative = 0)
        bitcoin_aggregate_score = functools.reduce(lambda a, b: a+b, bitcoin_results)
        ethereum_aggregate_score = functools.reduce(lambda a, b: a+b, ethereum_results)
        dogecoin_aggregate_score = functools.reduce(lambda a, b: a+b, dogecoin_results)
        chainlink_aggregate_score = functools.reduce(lambda a, b: a+b, chainlink_results)
        polkadot_aggregate_score = functools.reduce(lambda a, b: a+b, polkadot_results)

        model_run_coin_data = [
            {
                "name": 'bitcoin',
                "ticker": 'BTC',
                "score": bitcoin_aggregate_score,
                "timestamp": timestamp,
                "mentionCount": bitcoin_mention_count,
            },
            { 
                "name": 'ethereum',
                "ticker": 'ETH',
                "score": ethereum_aggregate_score,
                "timestamp": timestamp,
                "mentionCount": ethereum_mention_count,
            },
            { 
                "name": 'dogecoin',
                "ticker": 'DOGE',
                "score": dogecoin_aggregate_score,
                "timestamp": timestamp,
                "mentionCount": dogecoin_mention_count,
            },
            { 
                "name": 'chainlink',
                "ticker": 'LINK',
                "score": chainlink_aggregate_score,
                "timestamp": timestamp, 
                "mentionCount": chainlink_mention_count,
            },
            { 
                "name": 'polkadot',
                "ticker": 'DOT',
                "score": polkadot_aggregate_score,
                "timestamp": timestamp,
                "mentionCount": polkadot_mention_count,
            },
        ]

        end_time = datetime.datetime.now()
        print('process_message run time:', (end_time - start_time).total_seconds())

        print('OUTPUT:')
        convert_date_month = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        date_month = convert_date_month.strftime("%Y-%m")
        output_data = {
            "dateMonth": date_month,
            "raw": model_run_coin_data,
            "timestamp": timestamp
        }
        print(json.dumps(output_data, indent=2))
        # write model output to dynamodb
        write_to_db(output_data=output_data)

class ModelListener(SqsListener):
    def handle_message(self, body, attributes, messages_attributes):
        try:
            process_message(body, attributes, messages_attributes)
        except Exception as error:
            print('process_message failure')
            print(error)

listener = ModelListener(QUEUE_NAME, region_name=AWS_REGION)

if __name__ == "__main__":
    print('initializing SQS listener...')
    listener.listen()