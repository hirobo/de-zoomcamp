import faust

class TaxiRide(faust.Record, validation=True):
    PUlocationID: str

TOPIC = 'hirobo.taxi_ride_fhv_data_v2.json'
app = faust.App('hirobo.stream.v1', broker='kafka://localhost:9092')
topic = app.topic(TOPIC, value_type=TaxiRide)

pulocation_rides = app.Table('pulocation_rides', default=int)
counter = app.Table('counter', default=int)

#n_fhv = 23143222
#n_green = 630918
#n_total = n_fhv + n_green

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.PUlocationID):
        pulocation_rides[event.PUlocationID] += 1
        counter["counter"] += 1


@app.timer(600.0)
async def log_table_every_n_seconds():
    print(counter["counter"])
    l = list(pulocation_rides.items())

    if len(l) > 0:
        (id, value) = max(l, key=lambda a: a[1])
        print((id, value))


if __name__ == '__main__':
    app.main()
