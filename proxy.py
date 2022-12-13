import requests
from fastapi import FastAPI
import json
import redis

proxy = FastAPI()
# SERVER 1 - PORT 8000
# SERVER 2 - PORT 8001
# PROXY    - PORT 8002
servers = ["https://s1-pad-lab.herokuapp.com", "https://s2-pad-lab.herokuapp.com"]
current_server = 0
r = redis.Redis(host='redis-16831.c269.eu-west-1-3.ec2.cloud.redislabs.com', port=16831, password="QPnW6faQvgL5u2Cz3jcypQsVNGxaD1F4")
def synchronize_databases(type, current_server, data):
    if type == "POST":
        response = requests.post(f"{servers[current_server]}/data?person_id={data['PersonID']}&last_name={data['LastName']}&first_name={data['FirstName']}&address={data['Address']}&city={data['City']}")
        r = json.loads(response.text)
    if type == "DELETE":
        response = requests.delete(f"{servers[current_server]}/rm/{data['PersonID']}")

@proxy.get('/')
async def home():
    return {'message':'Welcome!'}

# route to receive all the data
@proxy.get('/data')
async def get_data():
    global current_server
    # check if data is not cached in redis
    data = r.get('data')
    if not data:  
        # get data from servers
        response = requests.get(f"{servers[current_server]}/data/")
        res = json.loads(response.text)
        print(res)
        # increment current_server to use the other database on the next request
        current_server = (current_server + 1) % len(servers)
        # cache data into redis
        r.set('data', json.dumps(res))
        r.expire('data', 10)
        return "FROM servers", res
    
    return "FROM REDIS: ", json.loads(data)


@proxy.get('/data/{id}')
async def get_data(id : int):
    global current_server
    # check if data is not cached in redis
    data = r.get(id)
    if not data:
        # get data from servers
        response = requests.get(f"{servers[current_server]}/data/{id}")
        res = json.loads(response.text)
        # increment current_server to use the other database on the next request
        current_server = (current_server + 1) % len(servers)
        # cache data in redis
        print("CACHED DATA INTO REDIS")
        r.set(id, json.dumps(res))
        r.expire(id, 10)
        return "FROM servers", res
    return "FROM REDIS", json.loads(data)
    
@proxy.post("/data")
def post_data(person_id: int, last_name: str, first_name: str, address: str, city: str):
    global current_server
    # post data to servers1
    response = requests.post(f"{servers[current_server]}/data?person_id={person_id}&last_name={last_name}&first_name={first_name}&address={address}&city={city}")
    r = json.loads(response.text)
    # increment current_server to use the other database on the next request
    current_server = (current_server + 1) % len(servers)
    # define the data to be passed to the function
    data = {
        "PersonID" : person_id,
        "LastName" : last_name,
        "FirstName": first_name,
        "Address" : address,
        "City" : city
    }
    synchronize_databases("POST", current_server, data)
    return r

@proxy.delete("/rm/{id}")
async def delete_data(id: int):
    global current_server
    data = r.get(id)
    if data:
        r.delete(id)
    response = requests.delete(f"{servers[current_server]}/rm/{id}")
    # increment current_server to use the other database on the next request
    current_server = (current_server + 1) % len(servers)
    # define the data to be passed to the function
    request_id = {"PersonID" : id}
    synchronize_databases("DELETE", current_server, request_id)
    return json.loads(response.text)

