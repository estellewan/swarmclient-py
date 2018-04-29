import asyncio
import websockets
import json
import uuid

DB_UUID = ""
WS_CONFIG = ""

def connect(ws, uuid):
    global DB_UUID
    DB_UUID = uuid
    global WS_CONFIG
    WS_CONFIG = ws

@asyncio.coroutine
async def create_send(future, key, value, request_id, ws=None):
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

def create_find_leader(key, value, request_id, leader_info=None):
    ws = WS_CONFIG
    if leader_info != None:
        ws_host = leader_info['leader-host']
        ws_port = leader_info['leader-port']
        ws =  "ws://" + ws_host + ":" + str(ws_port)

    loop = asyncio.get_event_loop()
    future = asyncio.Future()
    asyncio.ensure_future(create_send(future, key, value, request_id, ws))
    loop.run_until_complete(future)
    response = future.result()

    if 'error' in response and response['error'] == 'NOT_THE_LEADER':
        create_find_leader(key, value, request_id, response['data'])
    else:
        print(response)

def create(key, value):
    request_id = str(uuid.uuid4())

    create_find_leader(key, value, request_id)
