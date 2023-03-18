import csv
from json import dumps
from kafka import KafkaProducer


TOPIC = 'hirobo.taxi_ride_fhv_data_v2.json'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file_indexs = [
    ('../../resources/fhv_tripdata_2019-01.csv', 3),
    ('../../resources/green_tripdata_2019-01.csv', 5)
]


for file_index in file_indexs:
    file, index = file_index
    file = open(file)

    csvreader = csv.reader(file)
    header = next(csvreader)    
    for row in csvreader:
        if not row[index]:
            continue
        key = {"PUlocationID": int(row[index])}
        value = {"PUlocationID": int(row[index])}
        producer.send(TOPIC, value=value, key=key)


