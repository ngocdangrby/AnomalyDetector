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
        data = {'usage': 50+randint(0, 15),
                'timestamp': calendar.timegm(gmt),
                'source': topic_name}
        producer.send(topic_name, value=data)
        sleep(3)


processes = list()
for i in range(7):
    print("start cpu{}".format(i))
    p = multiprocessing.Process(target=produce_data, args=("ram{}".format(i),))
    processes.append(p)

for i, process in enumerate(processes):
    print("RUN cpu{}".format(i))
    process.start()


for i, process in enumerate(processes):
    print("JOIN cpu{}".format(i))
    process.join()
