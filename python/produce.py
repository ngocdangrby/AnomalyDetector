from time import sleep
from json import dumps
from kafka import KafkaProducer
import multiprocessing
from random import randint
import calendar
import time


def produce_data(topic_name):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    while(True):
        gmt = time.gmtime()
        data = {'usage': 50+randint(0, 5),
                'created_at': int(calendar.timegm(gmt)),
                'source_computer': topic_name}
        producer.send(topic_name, value=data)
        sleep(10)


processes = list()
for i in range(7):
    print("start testdata{}".format(i))
    p = multiprocessing.Process(target=produce_data, args=("testdata{}".format(i),))
    processes.append(p)

for i, process in enumerate(processes):
    print("RUN testdata{}".format(i))
    process.start()


for i, process in enumerate(processes):
    print("JOIN testdata{}".format(i))
    process.join()
