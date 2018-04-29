def get_ws_config(leader_info=None, WS_CONFIG=None):
    ws = WS_CONFIG
    if leader_info != None:
        ws_host = leader_info['leader-host']
        ws_port = leader_info['leader-port']
        ws =  "ws://" + ws_host + ":" + str(ws_port)

    return ws
