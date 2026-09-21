from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from pydantic import BaseModel
from starlette.types import Lifespan
import database


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages startup and shutdown lifecycle events."""
    await database.init_db()
    yield
    await database.close_db()

app = FastAPI(lifespan=lifespan)

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
