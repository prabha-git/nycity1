import uuid
from datetime import datetime
from pytz import timezone
import random
import time
from google.cloud import pubsub_v1
import json

project_id = 'nycity1'
topic_id='simulated_taxi_ride'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)

rides=[]

def generate_dropoff(ride_id):
    timestamp_utc = datetime.now(timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    ride_event = {"ride_id":ride_id,"timestamp_utc":timestamp_utc,"event_type":"dropoff"}
    return {'key':ride_id,'msg':json.dumps(ride_event)}


def generate_pickup():
    ride_id = str(uuid.uuid4())
    timestamp_utc = datetime.now(timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    ride_event = {"ride_id":ride_id,"timestamp_utc":timestamp_utc,"event_type":"pickup"}
    return {'key':ride_id,'msg':json.dumps(ride_event)}

def generate_n_pickup(n=10):
    n_rides=[]
    for i in range(n):
        r = generate_pickup()
        n_rides.append(r)
    return n_rides

    

if __name__ == '__main__':
    n_rides = generate_n_pickup(5)
    for i in n_rides:
        future = publisher.publish(topic_path,i['msg'].encode("utf-8"))
        #print(i['msg'].encode("utf-8"))
        #print(future.result())
        rides.append(i['key'])
    while True:
        r = generate_pickup()
        future = publisher.publish(topic_path,r['msg'].encode("utf-8"))
        #print(future.result())
        #print(r['msg'].encode("utf-8"))
        rides.append(r['key'])
        time.sleep(random.randrange(120))
        
        random_dropoff_id= rides.pop(random.randrange(len(rides)))
        r = generate_dropoff(random_dropoff_id)
        future = publisher.publish(topic_path,r['msg'].encode("utf-8"))
        time.sleep(random.randrange(120))
    
    