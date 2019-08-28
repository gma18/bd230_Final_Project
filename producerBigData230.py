import sys
import requests
from time import sleep
import time
import json
import random

from confluent_kafka import Producer
testing=True

if __name__ == '__main__':

    timeInterval = 61
    if testing:
        timeInterval=5
    print("Starting Producer .... ")

    topic = ''
    print("topic:"+topic)

    conf = {
        'bootstrap.servers': "",
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': "",
        'sasl.password': ""
    }
    p = Producer(**conf)

    print("Config:")
    print(conf)
    print("--------")


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))


    # api-endpoint
    # URLS = ["https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=INX&apikey=ZSUWKF1O98IHZYBF",
    #         "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol= &apikey=I0IKDQ0KZUOXEGAJ",
    #         "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=BA&apikey=0HQHVYPS3S1D9C6K"]
    URLS = ["https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=INX&apikey=7QOXG1MQC2CRWV63",
            "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol= &apikey=7QOXG1MQC2CRWV63",
            "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=BA&apikey=7QOXG1MQC2CRWV63"]

    #for e in range(5):
    while 1 == 1:
        # sending get request and saving the response as response object
        try:
            sendValue = {
                "timestamp": time.time()
            }
            if testing:
                sendValue["INX_perChange"] = str(random.randint(1,100)/100)
                sendValue["MSFT_perChange"] = str(random.randint(1,100)/100)
                sendValue["BA_perChange"] = str(random.randint(1,100)/100)
                sendValue["MSFT_vol"] = str(random.randint(1,100)*100)
            else:
                for url in URLS:
                    sleep(1)
                    r = requests.get(url=url)
                    # extracting data in json format

                    dataJson = r.json()
                    data = r.text
                    symbol=dataJson['Global Quote']['01. symbol']
                    percentChange=dataJson['Global Quote']['10. change percent'][:-1]
                    volume=dataJson['Global Quote']['06. volume']
                    sendValue[symbol+"_perChange"]=percentChange
                    sendValue[symbol+"_vol"] = volume


            sendValue_json = json.dumps(sendValue)
            print(sendValue_json)
            p.produce(topic, value=sendValue_json, callback=delivery_callback)
            sleep(timeInterval)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)

    sleep(5)
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
