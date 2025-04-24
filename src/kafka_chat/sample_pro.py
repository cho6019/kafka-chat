from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(bootstrap_servers='3.34.137.81:9092',
                         value_serializer=lambda v:json.dumps(v).encode('utf-8'))

for i in range(10000):
    msg = {"msg" : str(i)}
    producer.send('test', msg)

    if i % 100 == 99:
        producer.flush()
    time.sleep(0.01)

producer.flush()

