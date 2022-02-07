#!/usr/bin/env python3
from datetime import datetime
import time
import uuid
from sqsClient import SQSClient
import json
import boto3
from pyot.conf.pipeline import activate_pipeline, PipelineConf
from pyot.conf.model import activate_model, ModelConf
import asyncio
import traceback
import logging
from decimal import Decimal

@activate_model("riot")
class RiotModel(ModelConf):
    default_platform = "euw1"
    default_region = "europe"
    default_version = "latest"
    default_locale = "en_us"


@activate_model("lol")
class LolModel(ModelConf):
    default_platform = "euw1"
    default_region = "europe"
    default_version = "latest"
    default_locale = "en_us"

aws_region = "eu-central-1"

def getApiKey():
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=aws_region
    )
    return json.loads(client.get_secret_value(
        SecretId='riot/kla-key'
    )['SecretString'])['RIOT_KEY']


@activate_pipeline("lol")
class LolPipeline(PipelineConf):
    name = "lol_main"
    default = True
    stores = [
        {
            "backend": "pyot.stores.riotapi.RiotAPI",
            "log_level": 30,
            "api_key": getApiKey(),
            # "rate_limiter": {
            #     "backend": "pyot.limiters.redis.RedisLimiter",
            #     "host": os.environ["REDIS_HOST"],
            #     "port": os.environ["REDIS_PORT"],
            #     "db": 0,
            #     "ssl": True,
            #     "password": os.environ["REDIS_PASSWORD"]
            # },
            "rate_limiter": {
                "backend": "pyot.limiters.memory.MemoryLimiter"
            },
            "error_handler": {
                400: ("T", []),
                503: ("E", [3, 3])
            }
        }
    ]

from pyot.models import lol
from pyot.utils.lol.routing import platform_to_region

def import_match_from_message(body, table):
    matchId = body["matchId"]
    logging.info('importing %s', matchId)

    # check if db already contains this match.
    existingItem = table.get_item(Key={'matchId': matchId})
    if 'Item' in existingItem and existingItem['Item'] is not None:
        return True # already exists

    # pull from Riot
    loop = asyncio.get_event_loop()
    coroutine = lol.Match(matchId).get()
    match = loop.run_until_complete(coroutine).raw()
    
    match = json.loads(json.dumps(match), parse_float=Decimal)

    # rewrite the data a tiny bit
    info = match['info']
    info['dataVersion'] = match['metadata']['dataVersion']
    info['matchId'] = match['metadata']['matchId']
    info['platformId'] = info['platformId']
    info['imported'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    # write to table
    table.put_item(Item=info)

    return True

def import_summoner_from_message(body, summonerTable, matchTable, matchQueue, sqs):
    platform = body["platform"]
    puuid = body["puuid"]
    logging.info('importing %s from %s', puuid, platform)
    loop = asyncio.get_event_loop()

    # check if db already contains this match.
    existingItem = summonerTable.get_item(Key={'puuid': puuid, 'platformId': platform})
    if not 'Item' in existingItem or existingItem['Item'] is None:
        # summoner doesn't exist, import
        coroutine = lol.Summoner(puuid=puuid, platform=platform).get()
        summoner = loop.run_until_complete(coroutine).raw()

        summoner = json.loads(json.dumps(summoner), parse_float=Decimal)

        # rewrite the data a tiny bit
        summoner['imported'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        summoner['platformId'] = platform

        # write to table
        summonerTable.put_item(Item=summoner)

    # update matches
    start = 0
    groupId = str(uuid.uuid4())
    while True:
        history = lol.MatchHistory(puuid=puuid, region=platform_to_region(platform))
        history.query(start=start, count=100)
        start += 100
        history = loop.run_until_complete(history.get())
        for id in history.ids:
            existingItem = matchTable.get_item(Key={'matchId': id})
            if 'Item' in existingItem and existingItem['Item'] is not None:
                break
            sqs.send_message(QueueUrl=matchQueue, MessageBody='{"matchId":"' + id + '"}', MessageDeduplicationId=id, MessageGroupId=groupId)
        if len(history.ids) < 100:
            break

    return True

def try_import_matches(queueUrl, sqs, maxBatchSize, table):
    
    messages = sqs.get_next_messages(queueUrl, maxBatchSize)

    if len(messages) > 0:
        for msg in messages:
            import_match_from_message(json.loads(msg['Body']), table)
            sqs.delete_message(queueUrl, msg['ReceiptHandle'])

        return True
    return False

def try_import_summoners(queueUrl, sqs, maxBatchSize, table, matchTable, matchQueue):
    
    messages = sqs.get_next_messages(queueUrl, maxBatchSize)

    if len(messages) > 0:
        for msg in messages:
            import_summoner_from_message(json.loads(msg['Body']), table, matchTable, matchQueue, sqs)
            sqs.delete_message(queueUrl, msg['ReceiptHandle'])

        return True
    return False

def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    session = boto3.session.Session(region_name=aws_region)
    sqs = SQSClient(session, aws_region)
    matchQueue = sqs.get_queue("import-match-items.fifo")
    summonersQueue = sqs.get_queue("import-summoners-items.fifo")
    dynamo = session.resource('dynamodb', region_name=aws_region)
    matchTable = dynamo.Table('matches')
    summonersTable = dynamo.Table('summoners')
    logging.info('starting main loop')
    
    maxBatchSize = 3
    i = 2
    while True:
        try:
            if try_import_matches(matchQueue, sqs, maxBatchSize, matchTable):
                i = 2
                continue

            if try_import_summoners(summonersQueue, sqs, maxBatchSize, summonersTable, matchTable, matchQueue):
                i = 2
                continue
        except Exception as e:
            logging.error(traceback.format_exc())
            i *= 2

        i *= 2
        logging.info('nothing to do. Sleeping for %s', str(i))
        time.sleep(i)


if __name__ == "__main__":
    main()