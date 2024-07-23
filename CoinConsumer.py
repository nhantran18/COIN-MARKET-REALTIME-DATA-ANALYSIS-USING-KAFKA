# Databricks notebook source
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem
import os
import json
import multiprocessing
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')

s3 = S3FileSystem()
list_consumer = []

symbols = ['BTC', 'ETH', 'XRP', 'BCH', 'LTC', 'ADA', 'DOT', 'BNB', 'LINK', 'XLM']
for i in range (0, 10):
    list_consumer += [KafkaConsumer(symbols[i],
                         bootstrap_servers=[BOOTSTRAP_SERVERS],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))]


def saveData (consumer, symbol):
    for count, i in enumerate(consumer):
        with s3.open("s3://kafka-stock-market-storage-bigdataclass/{}{}.json".format(symbol,count), 'w') as file:
            json.dump(i.value, file)

p0 = multiprocessing.Process(target=saveData(list_consumer[0],'BTC'))
p1 = multiprocessing.Process(target=saveData(list_consumer[1],'ETH'))
p2 = multiprocessing.Process(target=saveData(list_consumer[2],'XRP'))
p3 = multiprocessing.Process(target=saveData(list_consumer[3],'BCH'))
p4 = multiprocessing.Process(target=saveData(list_consumer[4],'LTC'))
p5 = multiprocessing.Process(target=saveData(list_consumer[5],'ADA'))
p6 = multiprocessing.Process(target=saveData(list_consumer[6],'DOT'))
p7 = multiprocessing.Process(target=saveData(list_consumer[7],'BNB'))
p8 = multiprocessing.Process(target=saveData(list_consumer[8],'LINK'))
p9 = multiprocessing.Process(target=saveData(list_consumer[9],'XLM'))

# Khởi động các tiến trình
p0.start()
p1.start()
p2.start()
p3.start()
p4.start()
p5.start()
p6.start()
p7.start()
p8.start()
p9.start()


# Chờ các tiến trình kết thúc
p0.join()
p1.join()
p2.join()
p3.join()
p4.join()
p5.join()
p6.join()
p7.join()
p8.join()
p9.join()
