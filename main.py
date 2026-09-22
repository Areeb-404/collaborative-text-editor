from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.types import Lifespan
from typing import Dict, List, Set
import database


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages startup and shutdown lifecycle events."""
    await database.init_db()
    yield
    await database.close_db()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self) -> None:
        # stores active websocket connections grouped by doc_id
        self.active_rooms: Dict[int, List[WebSocket]] = {}
    
    async def connect(self, doc_id: int, websocket: WebSocket):
        await websocket.accept()
        if doc_id not in self.active_rooms:
            self.active_rooms[doc_id] = []
        self.active_rooms[doc_id].append(websocket)
    
    def disconnect(self, doc_id: int, websocket: WebSocket):
        if doc_id in self.active_rooms:
            if websocket in self.active_rooms[doc_id]:
                self.active_rooms[doc_id].remove(websocket)
            if not self.active_rooms[doc_id]:
                del self.active_rooms[doc_id]

    async def broadcast_to_room(self, doc_id: int, message: str, sender: WebSocket):
        """Broadcasts a message to all clients in a room Except the sender."""
        if doc_id in self.active_rooms:
            for conn in self.active_rooms[doc_id]:
                if conn != sender:
                    try:
                        await conn.send_text(message)
                    except Exception:
                        pass

manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            json_payload = await websocket.receive_json()
            await websocket.send_json(json_payload)
    except WebSocketDisconnect:
        pass
        

class document_create_request(BaseModel):
    title: str
    content: str=''

@app.post("/documents",status_code=status.HTTP_201_CREATED)
async def create_doc_endpoint(payload: document_create_request):
    document = await database.create_document(payload.title, payload.content)
    return document

@app.get("/documents/{doc_id}")
async def get_doc_endpoint(doc_id: int):
    document = await database.get_document(doc_id)
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="document not found!"
        )
    return document

@app.websocket("/ws/{doc_id}")
async def websocket_document_endpoint(websocket:WebSocket, doc_id: int):
    # verify if the doc exists in the database
    doc = await database.get_document(doc_id)
    if not doc:
        await websocket.close(code=4004, reason="Document Not Found.")
        return

    # connecting the client to the document room
    await manager.connect(doc_id,websocket)

    # send initial state of the doc fetched from the DB to the newly connected client
    await websocket.send_json({"type" : "init", "content" : doc["content"]})

    try:
        while True:
            data = await websocket.receive_json()

            # if the client sends an update message.
            if data.get("type") == "update":
                new_content = data.get("content","")
                await database.update_document(doc_id,new_content)

            # broadcast the update message to other collaborators in the same room
            await manager.broadcast_to_room(
                doc_id=doc_id,
                message=data.get("content",""),
                sender=websocket
            )
    except WebSocketDisconnect:
        manager.disconnect(doc_id,websocket)

