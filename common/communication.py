import asyncio
import websockets
import json
import uuid
import time
from communication_utils import *

DB_UUID = ""
WS_CONFIG = ""

def connect(ws, uuid):
    global DB_UUID
    DB_UUID = uuid
    global WS_CONFIG
    WS_CONFIG = ws

async def __create_send(future, key, value, request_id, ws=None):
    if ws is None:
        ws = WS_CONFIG

    async with websockets.connect(ws) as websocket:
        request = {
          "bzn-api": "crud",
          "cmd": "create",
          "data": {
              "key": key,
              "value": value
          },
          "db-uuid": DB_UUID,
          "request-id": request_id
        }
        await websocket.send(json.dumps(request))

        response_string = await websocket.recv()
        response = json.loads(response_string)

        future.set_result(response)

def __create(key, value, request_id, leader_info=None):
    ws = get_ws_config(leader_info, WS_CONFIG)

    loop = asyncio.get_event_loop()
    future = asyncio.Future()
    asyncio.ensure_future(__create_send(future, key, value, request_id, ws))
    loop.run_until_complete(future)
    response = future.result()

    if 'error' in response and response['error'] == 'NOT_THE_LEADER':
        __create(key, value, request_id, response['data'])
    else:
        print(response)

def create(key, value):
    request_id = str(uuid.uuid4())

    __create(key, value, request_id)

async def __read_send(future, key, request_id, ws=None):
    if ws is None:
        ws = WS_CONFIG

    async with websockets.connect(ws) as websocket:
        request = {
          "bzn-api": "crud",
          "cmd": "read",
          "data": {
              "key": key
          },
          "db-uuid": DB_UUID,
          "request-id": request_id
        }
        await websocket.send(json.dumps(request))

        response_string = await websocket.recv()
        response = json.loads(response_string)

        future.set_result(response)

def __read(key, request_id, leader_info=None):
    ws = get_ws_config(leader_info, WS_CONFIG)

    loop = asyncio.get_event_loop()
    future = asyncio.Future()
    asyncio.ensure_future(__read_send(future, key, request_id, ws))
    loop.run_until_complete(future)
    response = future.result()

    if 'error' in response and response['error'] == 'NOT_THE_LEADER':
        __read(key, request_id, response['data'])
    else:
        print(response)

def read(key):
    request_id = str(uuid.uuid4())

    __read(key, request_id)

async def __update_send(future, key, value, request_id, ws=None):
    if ws is None:
        ws = WS_CONFIG

    async with websockets.connect(ws) as websocket:
        request = {
          "bzn-api": "crud",
          "cmd": "update",
          "data": {
              "key": key,
              "value": value
          },
          "db-uuid": DB_UUID,
          "request-id": request_id
        }
        await websocket.send(json.dumps(request))

        response_string = await websocket.recv()
        response = json.loads(response_string)

        future.set_result(response)

def __update(key, value, request_id, leader_info=None):
    ws = get_ws_config(leader_info, WS_CONFIG)

    loop = asyncio.get_event_loop()
    future = asyncio.Future()
    asyncio.ensure_future(__update_send(future, key, value, request_id, ws))
    loop.run_until_complete(future)
    response = future.result()

    if 'error' in response and response['error'] == 'NOT_THE_LEADER':
        __update(key, value, request_id, response['data'])
    else:
        print(response)

def update(key, value):
    request_id = str(uuid.uuid4())

    __update(key, value, request_id)

# Testing - to be removed
connect('ws://13.78.131.94:51011', '80174b53-2dda-49f1-9d6a-6a780d4cceee')
create("hello", 4324234)
time.sleep(2)
read("hello")
time.sleep(2)
update("hello", 6565756765)
time.sleep(2)
read("hello")
