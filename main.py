import asyncio related to this document since all the users are disconnected
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.types import Lifespan
from typing import Awaitable, Dict, List, Set
import database
import json
import uuid

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages startup and shutdown lifecycle events."""
    await database.init_db()
    yield
    # flush all active rooms to the database
    for doc_id in list(manager.room_data.keys()):
        await manager.flush_to_db(doc_id)
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
        self.active_rooms: Dict[int, Dict[WebSocket, dict]] = {}
        # in memory document cache for each room
        self.room_data: Dict[int, str] = {}
        # Debounce timer tasks per doc id
        self.db_save_tasks: Dict[int, asyncio.Task] = {}
        # tracks if in memory content has uncommitted edits
        self.is_dirty: Dict[int, bool] = {}
    
    def connect(self, doc_id: int, websocket: WebSocket, user_info: Dict, initial_content: str):
        if doc_id not in self.active_rooms:
            self.active_rooms[doc_id] = {}
            self.room_data[doc_id] = initial_content
            self.is_dirty[doc_id] = False
        self.active_rooms[doc_id][websocket] = user_info

    async def disconnect(self, doc_id: int, websocket: WebSocket):
        if doc_id in self.active_rooms:
            if websocket in self.active_rooms[doc_id]:
                self.active_rooms[doc_id].pop(websocket, None)

            if not self.active_rooms[doc_id]: # when room becomes empty, flush pending edits and free memory
                del self.active_rooms[doc_id]

                # cancel pending timer and flush immediately
                if(
                    doc_id in self.db_save_tasks
                    and not self.db_save_tasks[doc_id].done()
                ):
                    self.db_save_tasks[doc_id].cancel()

                await self.flush_to_db(doc_id)
                # cleanup all memory related to this document since all the users are disconnected
                self.room_data.pop(doc_id, None)
                self.is_dirty.pop(doc_id, None)
                self.db_save_tasks.pop(doc_id, None)
            else:
                # if the room still has users, broadcast the updated presence list
                await self.broadcast_presence(doc_id)

    async def broadcast_to_room(self, doc_id: int, message: str, sender: WebSocket):
        """Broadcasts a message to all clients in a room Except the sender."""
        if doc_id in self.active_rooms:
            payload = json.dumps({"type": "update", "content" : message})
            for conn in self.active_rooms[doc_id]:
                if conn != sender:
                    try:
                        await conn.send_text(payload)
                    except Exception:
                        pass

    async def handle_update(self, doc_id: int, content: str, sender: WebSocket):
        """Updates memory state, broadcasts instantly, and resets DB timer"""
        # updating the memory state
        self.room_data[doc_id] = content
        self.is_dirty[doc_id] = True

        # broadcast to every client in the room
        await self.broadcast_to_room(doc_id,content,sender)

        # reset the DB timer
        if doc_id in self.db_save_tasks and not self.db_save_tasks[doc_id].done():
            self.db_save_tasks[doc_id].cancel()

        self.db_save_tasks[doc_id] = asyncio.create_task(
            self.debounced_db_save(doc_id, delay=2.0)
        )

    async def debounced_db_save(self, doc_id: int, delay: float = 2.0):
        """This function keeps scheduling a save at every keystroke, but if a new keystroke comes 
        in before that 2-second timer finishes, the old timer is cancelled and a new one takes its place. 
        This keeps happening indefinitely until a keystroke is followed by 2 full seconds of silence, 
        only then does the timer actually complete and trigger"""
        try:
            await asyncio.sleep(delay)
            await self.flush_to_db(doc_id)
        except asyncio.CancelledError:
            # timer has been reset by an upcoming update
            pass

    async def flush_to_db(self, doc_id: int):
        """Executes SQL updates if there are uncommitted edits"""
        if self.is_dirty.get(doc_id, False) and doc_id in self.room_data:
            content = self.room_data[doc_id]
            self.is_dirty[doc_id] = False # the document is already not dirty since it is being immediately committed in the next line
            await database.update_document(doc_id, content)
            print(f"--> [DB Flush] Successfully saved document {doc_id} to the database.")

    async def broadcast_presence(self, doc_id: int):
        """Sends list of current users to everyone in the room."""
        if doc_id in self.active_rooms:
            active_users = list(self.active_rooms[doc_id].values())
            presence_payload = {
                "type" : "presence",
                "users" : active_users,
                "count" : len(active_users),
            }
            for connection in self.active_rooms[doc_id]:
                try:
                    await connection.send_json(presence_payload)
                except Exception:
                    pass

                        
manager = ConnectionManager()


class document_create_request(BaseModel):
    title: str
    content: str=''

@app.post("/documents",status_code=status.HTTP_201_CREATED)
async def create_doc_endpoint(payload: document_create_request):
    document = await database.create_document(payload.title, payload.content)
    return document

@app.get("/documents/{doc_id}")
async def get_doc_endpoint(doc_id: int):
    # return in memory if room is active, else from the DB
    if doc_id in manager.room_data:
        return {"id": doc_id, "content": manager.room_data[doc_id]}

    # if inactive then fetch from DB
    document = await database.get_document(doc_id)
    if not document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found."
        )
    return document

@app.websocket("/ws/{doc_id}")
async def websocket_document_endpoint(websocket:WebSocket, doc_id: int):
    await websocket.accept()
    # create a unique user indentity as soon as a connection is established.
    user_tag = uuid.uuid4().hex[:4]
    user_info = {"id" : f"usr_{user_tag}", "name" : f"User-{user_tag}"}

    # Load content from memory if room is active, otherwise fetch from DB
    if doc_id in manager.room_data:
        current_content = manager.room_data[doc_id]
    else:
        document = await database.get_document(doc_id)
        if not document:
            await websocket.close(code=4004, reason="Document Not Found.")
            return
        current_content = document["content"]

    # register connection in room manager
    manager.connect(doc_id, websocket, user_info, current_content)
    
    # send initial document state
    await websocket.send_json({"type" : "init", "content" : current_content})

    # broadcast presence to everyone in the room
    await manager.broadcast_presence(doc_id)

    try:
        while True:
            data = await websocket.receive_json()

            # if the client sends an update message.
            if data.get("type") == "update":
                new_content = data.get("content","")
                await manager.handle_update(doc_id=doc_id, content=new_content, sender=websocket)

    except WebSocketDisconnect:
        await manager.disconnect(doc_id,websocket)

