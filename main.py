from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            json_payload = await websocket.receive_json()
            await websocket.send_json(json_payload)
    except WebSocketDisconnect:
        pass
