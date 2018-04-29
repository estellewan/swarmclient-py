import asyncio
import websockets
import json
import uuid

DB_UUID = ""
WS_CONFIG = ""

async def send(key, value, request_id):
    async with websockets.connect(WS_CONFIG) as websocket:
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

        print (response)

def connect(ws, uuid):
    global DB_UUID
    DB_UUID = uuid
    global WS_CONFIG
    WS_CONFIG = ws

def create(key, value):
    request_id = str(uuid.uuid4())
    asyncio.get_event_loop().run_until_complete(send(key, value, request_id))
