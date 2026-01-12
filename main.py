import os
import traceback
import json
from collections import deque
from typing import List, Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Project imports
from create_pool import create_pool
from vector_search import run_vector_search
from graph_search import get_neighbors_logic
from Entities import entry_point
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Config ----------
OWNER = os.environ.get("DB_OWNER", "ADMIN")
GRAPH_NAME = os.environ.get("GRAPH_NAME", "SUPPLY_CHAIN_GRAPH_NEW_APPROACH")
PORT = int(os.environ.get("PORT", 5000))

# ---------- Global state ----------
APP_STATE: Dict[str, Any] = {"pool": None, "last_query": None}
GLOBAL_QUEUE = deque()
VISITED_NODES = set()
CONSOLIDATED_CONTEXT: List[Dict[str, Any]] = []
NEIGHBOR_CACHE: Dict[str, Dict[str, Any]] = {}

# --- GLOBAL VARIABLES ---
LATEST_NEIGHBOR_RESULT: List[Dict[str, Any]] = []
GLOBAL_SELECTED_RESULTS: List[Dict[str, Any]] = []

# NEW: Stores the list of nodes to be processed in the current/next iteration
CURRENT_ITERATION_NODES: List[Dict[str, Any]] = []
# ------------------------


# ---------- FastAPI app & lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 API Starting...")
    try:
        APP_STATE["pool"] = create_pool()
        print("✅ Pool initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to init pool: {e}")
    yield
    print("🛑 API Stopping...")
    if APP_STATE["pool"]:
        APP_STATE["pool"].close()


app = FastAPI(title="Graph Agent API (batched get_neighbors)",
              lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request,
                                       exc: RequestValidationError):
    body = await request.body()
    print("🔥 422 Validation Error!")
    print("URL:", request.url)
    print("Request body (raw):", body)
    print("Validation errors:", exc.errors())
    return JSONResponse(status_code=422,
                        content={
                            "status": "error",
                            "message": "Unprocessable Entity",
                            "details": exc.errors()
                        })


# ---------- Pydantic models ----------
class VectorSearchRequest(BaseModel):
    query: str


class SelectionItem(BaseModel):
    source_table: str
    source_id: Any
    selected_indices: Any = ""


class UpdateQueueRequest(BaseModel):
    selections: List[SelectionItem]


class NodeItem(BaseModel):
    table_name: str
    node_id: Any


class GetNeighborsRequest(BaseModel):
    nodes: List[NodeItem]


# 1. Define a schema for your request body
class SearchRequest(BaseModel):
    query: str


# ---------- Utilities ----------
def make_key(table: str, node_id: Any) -> str:
    return f"{table}_{node_id}"


# ---------- Endpoints ----------


@app.post("/vector_search")
def api_vector_search(request: SearchRequest):
    """
    1. Runs vector search.
    2. Resets global state.
    3. Populates CURRENT_ITERATION_NODES with search results.
    """
    pool = APP_STATE.get("pool")
    if not pool:
        raise HTTPException(status_code=503, detail="DB not connected")

    query = request.query
    query = (query or "").strip()
    if not query:
        raise HTTPException(status_code=400,
                            detail="Query must be a non-empty string")

    try:
        with pool.acquire() as conn:
            results = run_vector_search(conn, query)

        seen_in_results = set()

        for item in results:
            t = item.get("table_name")
            nid = item.get("node_id")
            if t is None or nid is None:
                continue

            key = make_key(t, nid)
            if key not in seen_in_results:
                seen_in_results.add(key)

        APP_STATE["last_query"] = query

        print(
            f"✅ Vector Search Complete. CURRENT_ITERATION_NODES set to {len(CURRENT_ITERATION_NODES)} items."
        )

        return {"vector_results": results}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_neighbors_post")
def api_get_neighbors(payload: Dict[str, Any]):
    """
    Accepts a JSON body with `nodes: [{table_name, node_id}, ...]`.
    Processes all nodes in parallel, fetches neighbors, caches results,
    appends to `LATEST_NEIGHBOR_RESULT`, and returns an array of neighbor objects.
    """
    # 1. Get the nodes 'string'
    nodes_raw = payload.get('nodes')

    # 2. If it's a string, we MUST parse it into a list
    if isinstance(nodes_raw, str):
        try:
            input_nodes = json.loads(nodes_raw)
        except json.JSONDecodeError:
            return {"error": "Invalid JSON format in nodes string"}
    else:
        # If it's already a list, just use it
        input_nodes = nodes_raw

    print(f"✅ Successfully parsed {(input_nodes)} nodes.")

    pool = APP_STATE.get("pool")

    def process_node(node):
        """
        Process a single node: fetch neighbors, cache, and return result.
        Runs in a worker thread.
        """
        table = node.get("table_name") if isinstance(node, dict) else getattr(
            node, "table_name", None)
        nid = node.get("node_id") if isinstance(node, dict) else getattr(
            node, "node_id", None)
        if table is None or nid is None:
            return {
                "source_table": table,
                "source_id": nid,
                "error": "Missing table_name or node_id"
            }
        try:
            real_nid = int(nid)
        except Exception:
            real_nid = nid
        cache_key = make_key(table, real_nid)
        # skip if already visited
        if cache_key in VISITED_NODES:
            return {
                "source_table": table,
                "source_id": real_nid,
                "status": "skipped_already_visited"
            }
        # mark visited
        VISITED_NODES.add(cache_key)
        try:
            with pool.acquire() as conn:
                result = get_neighbors_logic(conn, table, real_nid, OWNER,
                                             GRAPH_NAME)
            full_neighbors = result.get("neighbor_results", []) or []
            node_entry = {
                "source_table": table,
                "source_id": real_nid,
                "source_row_data": result.get("source_row_data"),
                "neighbor_results": full_neighbors
            }
            return node_entry
        except Exception as e:
            traceback.print_exc()
            return {
                "source_table": table,
                "source_id": real_nid,
                "error": str(e)
            }

    response_nodes: List[Dict[str, Any]] = []
    try:
        # Process all nodes in parallel
        with ThreadPoolExecutor(
                max_workers=min(10, len(input_nodes))) as executor:
            futures = {
                executor.submit(process_node, node): node
                for node in input_nodes
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    # Append to global if it's a successful result (no error/skipped)
                    if "error" not in result and result.get(
                            "status") != "skipped_already_visited":
                        response_nodes.append(result)
                except Exception as e:
                    traceback.print_exc()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    print("Procsses all the nodes and the neighbours are : ", response_nodes)

    return {"status": "success", "neighbors": response_nodes}


class EntityCreate(BaseModel):
    result: Any


@app.post("/entity")
def create_entity(payload: EntityCreate):
    pool = APP_STATE.get("pool")

    with pool.acquire() as conn:
        entry_point(conn, payload.result)

    return {"status": "success"}


if __name__ == "__main__":
    print("started")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
