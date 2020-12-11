from time import sleep
from json import dumps
from kafka import KafkaProducer
import multiprocessing
from random import randint
import calendar
import time
import pandas as pd


def produce_data(topic_name):

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    while(True):
        df = pd.read_csv("base.csv")
        df = df.set_index(df["name"])
        gmt = time.gmtime()
        base = int(df.loc[topic_name][1])
        data = {'pkts': base +randint(0, 5),
                'timestamp': int(calendar.timegm(gmt))*1000,
                'interface': topic_name,
                'hello':"test"}
        producer.send(topic_name, value=data)
        sleep(2)


processes = list()
for i in range(7):
    print("start testdata{}".format(i))
    p = multiprocessing.Process(
        target=produce_data, args=("testinterface{}".format(i),))
    processes.append(p)

for i, process in enumerate(processes):
    print("RUN testdata{}".format(i))
    process.start()


for i, process in enumerate(processes):
    print("JOIN testdata{}".format(i))
    process.join()