import os
import asyncpg
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/collab_db"
)

pool: Optional[asyncpg.Pool] = None

async def init_db() -> None:
    """Initialises the database connection pool and creates the document schema."""
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS documents(
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT DEFAULT '',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);
        """)

async def close_db():
    """terminates all the active connections in the pool correctly"""
    global pool
    if pool:
        await pool.close()

async def create_document(title: str, content: str="") -> Dict[str,Any]:
    """Inserts a new document record into postgreSQL"""

    if pool is None:
        raise RuntimeError("Database connection is not initialised.")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO documents (title, content)
            VALUES ($1,$2)
            RETURNING id, content, created_at;
        """,title,content)
        return dict(row) if row else{}


async def get_document(doc_id: int) -> Optional[Dict[str,Any]]:
    """Retrieves a document record by ID"""
    
    if pool is None:
        raise RuntimeError("Database connection is not initialised.")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id, title, content, created_at FROM documents WHERE id = $1;
        """,doc_id)
        return dict(row) if row else None

async def update_document(doc_id: int, content: str) -> Optional[Dict[str,Any]]:
    """Updates document content in postgreSQL by id"""
    if pool is None:
        raise RuntimeError("Database connection pool is not initialised")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE documents
            SET content = $1
            WHERE id = $2
            RETURNING id, title, content, created_at;
            """,
        content,doc_id)
    return dict(row) if row else None


