from time import sleep
from json import dumps
from kafka import KafkaProducer
import multiprocessing
from random import randint
import calendar
import time
import pandas as pd

plugins = ["cpu", "ram", "disk"]


def produce_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    while(True):
        df = pd.read_csv("base_data.csv")
        df = df.set_index(df["name"])
        gmt = time.gmtime()
        comname = str("com" + str(randint(0, 5)))
        pluginID = randint(0, 2)
        pluginName = plugins[pluginID]
        base = int(df.loc[comname][pluginID+1])
        data = [{'values': base + randint(0, 5),
                 "timestamp": int(calendar.timegm(gmt))*1000,
                 "interface": comname,
                 "plugin": pluginName
                 }]
        producer.send("input", value=data)
        sleep(0.1)


processes = list()
for i in range(2):
    print("start testdata{}".format(i))
    p = multiprocessing.Process(
        target=produce_data)
    processes.append(p)

for i, process in enumerate(processes):
    print("RUN testdata{}".format(i))
    process.start()


for i, process in enumerate(processes):
    print("JOIN testdata{}".format(i))
    process.join()
