# import os
# import traceback
# import json
# from collections import deque
# from typing import List, Any, Dict, Optional

# from fastapi import FastAPI, HTTPException, Request
# from fastapi.responses import JSONResponse
# from fastapi.exceptions import RequestValidationError
# from pydantic import BaseModel
# from contextlib import asynccontextmanager

# # Project imports (must exist in your project)
# from create_pool import create_pool
# from vector_search import run_vector_search
# from graph_search import get_neighbors_logic

# # ---------- Config ----------
# OWNER = os.environ.get("DB_OWNER", "ADMIN")
# GRAPH_NAME = os.environ.get("GRAPH_NAME", "SUPPLY_CHAIN_GRAPH_NEW_APPROACH")
# PORT = int(os.environ.get("PORT", 5000))

# # ---------- Global state ----------
# APP_STATE: Dict[str, Any] = {"pool": None, "last_query": None}
# GLOBAL_QUEUE = deque(
# )  # items: {"table_name":..., "node_id":..., "depth": int}
# VISITED_NODES = set()  # set of "table_nodeid" keys
# CONSOLIDATED_CONTEXT: List[Dict[str, Any]] = []  # append-only
# NEIGHBOR_CACHE: Dict[str, Dict[str, Any]] = {
# }  # key -> {"source":..., "neighbors":[...], "stored_depth": int}

# # --- GLOBAL VARIABLE (LIST) ---
# # Stores history of all results appended over time.
# LATEST_NEIGHBOR_RESULT: List[Dict[str, Any]] = []
# # ------------------------------

# # ---------- FastAPI app & lifespan ----------
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     print("🚀 API Starting...")
#     try:
#         APP_STATE["pool"] = create_pool()
#         print("✅ Pool initialized successfully.")
#     except Exception as e:
#         print(f"❌ Failed to init pool: {e}")
#     yield
#     print("🛑 API Stopping...")
#     if APP_STATE["pool"]:
#         APP_STATE["pool"].close()

# app = FastAPI(title="Graph Agent API (batched get_neighbors)",
#               lifespan=lifespan)

# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request: Request,
#                                        exc: RequestValidationError):
#     body = await request.body()
#     print("🔥 422 Validation Error!")
#     print("URL:", request.url)
#     print("Request body (raw):", body)
#     print("Validation errors:", exc.errors())
#     return JSONResponse(status_code=422,
#                         content={
#                             "status": "error",
#                             "message": "Unprocessable Entity",
#                             "details": exc.errors()
#                         })

# # ---------- Pydantic models ----------
# class VectorSearchRequest(BaseModel):
#     query: str

# class SelectionItem(BaseModel):
#     source_table: str
#     source_id: Any
#     selected_indices: Any = ""

# class UpdateQueueRequest(BaseModel):
#     selections: List[SelectionItem]

# class NodeItem(BaseModel):
#     table_name: str
#     node_id: Any

# class GetNeighborsRequest(BaseModel):
#     nodes: List[NodeItem]

# # ---------- Utilities ----------
# def make_key(table: str, node_id: Any) -> str:
#     return f"{table}_{node_id}"

# # ... (parse_indices function remains the same) ...

# # ---------- Endpoints ----------

# @app.get("/vector_search/{query}")
# def api_vector_search(query: str):
#     """
#     Run vector search, seed GLOBAL_QUEUE, reset run state.
#     """
#     pool = APP_STATE.get("pool")
#     if not pool:
#         raise HTTPException(status_code=503, detail="DB not connected")

#     query = (query or "").strip()
#     if not query:
#         raise HTTPException(status_code=400,
#                             detail="Query must be a non-empty string")

#     # reset run-specific state
#     global GLOBAL_QUEUE, VISITED_NODES, CONSOLIDATED_CONTEXT, NEIGHBOR_CACHE, LATEST_NEIGHBOR_RESULT
#     GLOBAL_QUEUE = deque()
#     VISITED_NODES = set()
#     CONSOLIDATED_CONTEXT = []
#     NEIGHBOR_CACHE = {}

#     # RESET the global list for a new search session
#     LATEST_NEIGHBOR_RESULT = []

#     try:
#         with pool.acquire() as conn:
#             results = run_vector_search(conn, query)

#         start_items = []
#         for item in results:
#             t = item.get("table_name")
#             nid = item.get("node_id")
#             if t is None or nid is None:
#                 continue
#             key = make_key(t, nid)
#             if key not in VISITED_NODES:
#                 # We mark vector search results as visited so they aren't re-processed redundantly
#                 # unless logic dictates otherwise.
#                 GLOBAL_QUEUE.append({
#                     "table_name": t,
#                     "node_id": nid,
#                     "depth": 0
#                 })
#                 start_items.append({"table_name": t, "node_id": nid})

#         APP_STATE["last_query"] = query

#         return {
#             "vector_results": results,
#         }

#     except Exception as e:
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/get_neighbors/{table_name}/{node_id}")
# def api_get_neighbors(table_name: str, node_id: str):
#     """
#     1. Checks if node is already visited. If yes, skips.
#     2. Marks node as visited.
#     3. Fetches neighbors.
#     4. Appends to global list.
#     """
#     global LATEST_NEIGHBOR_RESULT, VISITED_NODES

#     pool = APP_STATE.get("pool")
#     if not pool:
#         raise HTTPException(status_code=503, detail="DB not connected")

#     try:
#         real_node_id = int(node_id)
#     except ValueError:
#         real_node_id = node_id

#     # Construct the unique key for this node
#     cache_key = make_key(table_name, real_node_id)

#     # --- CHECK IF VISITED ---
#     if cache_key in VISITED_NODES:
#         print(f"⏭️ Node {cache_key} already visited. Skipping.")
#         return {
#             "status": "skipped",
#             "message": f"Node {table_name} - {real_node_id} already visited."
#         }

#     # --- MARK AS VISITED ---
#     # We mark it visited now so subsequent calls (even if concurrent) know it's being handled
#     VISITED_NODES.add(cache_key)
#     print(f"📍 Visiting node: {cache_key}")

#     # 1. Wrap the single path parameter into the list structure logic expects
#     current_batch_nodes = [{"table_name": table_name, "node_id": real_node_id}]

#     response_nodes: List[Dict[str, Any]] = []

#     try:
#         with pool.acquire() as conn:
#             for n in current_batch_nodes:
#                 table = n["table_name"]
#                 nid = n["node_id"]

#                 # Fetch neighbors using existing logic
#                 try:
#                     result = get_neighbors_logic(conn, table, nid, OWNER,
#                                                  GRAPH_NAME)
#                 except Exception as e:
#                     traceback.print_exc()
#                     response_nodes.append({
#                         "source_table": table,
#                         "source_id": nid,
#                         "error": str(e)
#                     })
#                     continue

#                 # Cache results
#                 NEIGHBOR_CACHE[cache_key] = {
#                     "source": result.get("source_row_data"),
#                     "neighbors": result.get("neighbor_results", []),
#                 }

#                 full_neighbors = result.get("neighbor_results", []) or []

#                 response_nodes.append({
#                     "source_table":
#                     table,
#                     "source_id":
#                     nid,
#                     "source_row_data":
#                     result.get("source_row_data"),
#                     "neighbor_results":
#                     full_neighbors
#                 })

#     except Exception as e:
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=str(e))

#     # --- APPEND TO GLOBAL VARIABLE ---
#     if response_nodes:
#         LATEST_NEIGHBOR_RESULT.append(response_nodes[0])
#         print(
#             f"✅ Data APPENDED to LATEST_NEIGHBOR_RESULT (Total items: {len(LATEST_NEIGHBOR_RESULT)})"
#         )
#     else:
#         print("⚠️ No data found to store.")

#     print(LATEST_NEIGHBOR_RESULT)

#     return {
#         "status":
#         "success",
#         "message":
#         f"Neighbor data for {table_name} - {real_node_id} appended to global state."
#     }

# # --- NEW ENDPOINT TO GET THE ARRAY ---
# @app.get("/get_latest_neighbor_result")
# def api_get_latest_neighbor_result():
#     """
#     Returns the accumulated list of neighbor results.
#     """
#     global LATEST_NEIGHBOR_RESULT
#     return {
#         "status": "success",
#         "count": len(LATEST_NEIGHBOR_RESULT),
#         "data": LATEST_NEIGHBOR_RESULT
#     }

# @app.post("/send_indices")
# def receive_and_print_indices(payload: Dict[str, Any]):
#     # Just print the raw input string
#     print(payload)

#     return {"status": "printed"}

# # -------------------------------------

# # ---------- Run ----------
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=PORT)

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


# ---------- Utilities ----------
def make_key(table: str, node_id: Any) -> str:
    return f"{table}_{node_id}"


# ---------- Endpoints ----------


@app.get("/vector_search/{query}")
def api_vector_search(query: str):
    """
    1. Runs vector search.
    2. Resets global state.
    3. Populates CURRENT_ITERATION_NODES with search results.
    """
    pool = APP_STATE.get("pool")
    if not pool:
        raise HTTPException(status_code=503, detail="DB not connected")

    query = (query or "").strip()
    if not query:
        raise HTTPException(status_code=400,
                            detail="Query must be a non-empty string")

    global GLOBAL_QUEUE, VISITED_NODES, CONSOLIDATED_CONTEXT, NEIGHBOR_CACHE
    global LATEST_NEIGHBOR_RESULT, GLOBAL_SELECTED_RESULTS, CURRENT_ITERATION_NODES

    # Reset State
    GLOBAL_QUEUE = deque()
    VISITED_NODES = set()
    CONSOLIDATED_CONTEXT = []
    NEIGHBOR_CACHE = {}
    LATEST_NEIGHBOR_RESULT = []
    GLOBAL_SELECTED_RESULTS = []
    CURRENT_ITERATION_NODES = []  # Reset current nodes

    try:
        with pool.acquire() as conn:
            results = run_vector_search(conn, query)

        start_items = []
        seen_in_results = set()

        for item in results:
            t = item.get("table_name")
            nid = item.get("node_id")
            if t is None or nid is None:
                continue

            key = make_key(t, nid)
            if key not in seen_in_results:
                seen_in_results.add(key)

                # Create node object
                node_obj = {"table_name": t, "node_id": nid}

                GLOBAL_QUEUE.append({**node_obj, "depth": 0})
                start_items.append(node_obj)

                # Update CURRENT_ITERATION_NODES with initial search results
                CURRENT_ITERATION_NODES.append(node_obj)

        APP_STATE["last_query"] = query

        print(
            f"✅ Vector Search Complete. CURRENT_ITERATION_NODES set to {len(CURRENT_ITERATION_NODES)} items."
        )

        return {"vector_results": results}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_neighbors/{table_name}/{node_id}")
def api_get_neighbors(table_name: str, node_id: str):
    global LATEST_NEIGHBOR_RESULT, VISITED_NODES

    pool = APP_STATE.get("pool")
    if not pool:
        raise HTTPException(status_code=503, detail="DB not connected")

    try:
        real_node_id = int(node_id)
    except ValueError:
        real_node_id = node_id

    cache_key = make_key(table_name, real_node_id)

    if cache_key in VISITED_NODES:
        print(f"⏭️ Node {cache_key} already visited. Skipping.")
        return {
            "status": "skipped",
            "message": f"Node {table_name} - {real_node_id} already visited."
        }

    VISITED_NODES.add(cache_key)
    print(f"📍 Visiting node: {cache_key}")

    current_batch_nodes = [{"table_name": table_name, "node_id": real_node_id}]
    response_nodes: List[Dict[str, Any]] = []

    try:
        with pool.acquire() as conn:
            for n in current_batch_nodes:
                table = n["table_name"]
                nid = n["node_id"]
                try:
                    result = get_neighbors_logic(conn, table, nid, OWNER,
                                                GRAPH_NAME)
                except Exception as e:
                    traceback.print_exc()
                    response_nodes.append({
                        "source_table": table,
                        "source_id": nid,
                        "error": str(e)
                    })
                    continue

                NEIGHBOR_CACHE[cache_key] = {
                    "source": result.get("source_row_data"),
                    "neighbors": result.get("neighbor_results", []),
                }

                full_neighbors = result.get("neighbor_results", []) or []
                response_nodes.append({
                    "source_table":
                    table,
                    "source_id":
                    nid,
                    "source_row_data":
                    result.get("source_row_data"),
                    "neighbor_results":
                    full_neighbors
                })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    if response_nodes:
        LATEST_NEIGHBOR_RESULT.append(response_nodes[0])
        print(
            f"✅ Data APPENDED to LATEST_NEIGHBOR_RESULT (Total items: {len(LATEST_NEIGHBOR_RESULT)})"
        )
    else:
        print("⚠️ No data found to store.")

    return {
        "status":
        "success",
        "message":
        f"Neighbor data for {table_name} - {real_node_id} appended to global state."
    }


@app.get("/get_latest_neighbor_result")
def api_get_latest_neighbor_result():
    global LATEST_NEIGHBOR_RESULT
    return {
        "status": "success",
        "count": len(LATEST_NEIGHBOR_RESULT),
        "data": LATEST_NEIGHBOR_RESULT
    }


# --- NEW ENDPOINT TO GET CURRENT NODES ---
@app.get("/get_current_nodes")
def api_get_current_nodes():
    """
    Returns the list of nodes that need to be processed (either from vector search or next iteration).
    """
    global CURRENT_ITERATION_NODES
    print("yes it is ", CURRENT_ITERATION_NODES)
    return {
        "status": "success",
        "count": len(CURRENT_ITERATION_NODES),
        "nodes": CURRENT_ITERATION_NODES
    }


# -----------------------------------------

# @app.post("/send_indices")
# def receive_and_process_indices(payload: Dict[str, Any]):
#     """
#     1. Parses the selected indices.
#     2. Stores filtered results in GLOBAL_SELECTED_RESULTS.
#     3. Compiles 'next_unvisited_nodes'.
#     4. UPDATES CURRENT_ITERATION_NODES with these new unvisited nodes.
#     5. Resets LATEST_NEIGHBOR_RESULT.
#     """
#     global LATEST_NEIGHBOR_RESULT, GLOBAL_SELECTED_RESULTS, VISITED_NODES, CURRENT_ITERATION_NODES

#     print("📩 Received indices payload...", payload)

#     selections = []
#     if "result" in payload and "selections" in payload["result"]:
#         selections = payload["result"]["selections"]
#     elif "selections" in payload:
#         selections = payload["selections"]
#     else:
#         print("⚠️ No 'selections' found in payload structure.")
#         return {"status": "error", "message": "Invalid payload format"}

#     count_processed = 0
#     next_unvisited_nodes: List[Dict[str, Any]] = []

#     for sel in selections:
#         target_table = str(sel.get("source_table", "")).strip().upper()
#         target_id = str(sel.get("source_id"))
#         indices_str = str(sel.get("selected_indices", ""))

#         try:
#             indices = [
#                 int(i.strip()) for i in indices_str.split(",")
#                 if i.strip().isdigit()
#             ]
#         except Exception:
#             indices = []

#         matching_node = None
#         for node in LATEST_NEIGHBOR_RESULT:
#             node_table = str(node.get("source_table", "")).strip().upper()
#             node_id = str(node.get("source_id"))

#             if node_table == target_table and node_id == target_id:
#                 matching_node = node
#                 break

#         if matching_node:
#             all_neighbors = matching_node.get("neighbor_results", [])
#             selected_neighbors = []

#             for idx in indices:
#                 if 0 <= idx < len(all_neighbors):
#                     nb = all_neighbors[idx]
#                     selected_neighbors.append(nb)

#                     next_table = nb.get("target_table")
#                     next_id = nb.get("target_id")

#                     if next_table and next_id:
#                         next_key = make_key(next_table, next_id)
#                         if next_key not in VISITED_NODES:
#                             next_unvisited_nodes.append({
#                                 "table_name": next_table,
#                                 "node_id": next_id
#                             })

#             entry = {
#                 "source_table": matching_node.get("source_table"),
#                 "source_id": target_id,
#                 "source_row_data": matching_node.get("source_row_data"),
#                 "selected_neighbors": selected_neighbors
#             }
#             GLOBAL_SELECTED_RESULTS.append(entry)
#             count_processed += 1

#     unique_next_nodes = []
#     seen_next = set()
#     for item in next_unvisited_nodes:
#         k = make_key(item["table_name"], item["node_id"])
#         if k not in seen_next:
#             seen_next.add(k)
#             unique_next_nodes.append(item)

#     print(
#         f"✅ Processed {count_processed} items. Found {len(unique_next_nodes)} NEW unvisited nodes."
#     )

#     # --- UPDATE GLOBAL VARIABLE FOR NEXT ITERATION ---
#     CURRENT_ITERATION_NODES = unique_next_nodes
#     print(
#         f"🔄 CURRENT_ITERATION_NODES updated. New count: {len(CURRENT_ITERATION_NODES)}"
#     )

#     LATEST_NEIGHBOR_RESULT = []
#     print("🧹 LATEST_NEIGHBOR_RESULT has been reset to [].")

#     return {
#         "status": "success",
#         "processed_count": count_processed,
#         "global_total_stored": len(GLOBAL_SELECTED_RESULTS),
#         "next_unvisited_nodes": unique_next_nodes
#     }


@app.post("/send_indices")
def receive_and_process_indices(payload: Dict[str, Any]):
    """
    1. Parses selections.
    2. Stores selected data.
    3. Adds ALL selected neighbors to next iteration list (NO VISITED CHECK, NO DEDUPLICATION).
    """
    global LATEST_NEIGHBOR_RESULT, GLOBAL_SELECTED_RESULTS, VISITED_NODES, CURRENT_ITERATION_NODES

    print("hii", payload)

    # 1. Parse selections
    selections = []
    if "result" in payload and "selections" in payload["result"]:
        selections = payload["result"]["selections"]
    elif "selections" in payload:
        selections = payload["selections"]
    else:
        return {"status": "error", "message": "Invalid payload format"}

    count_processed = 0
    next_unvisited_nodes: List[Dict[str, Any]] = []

    # 2. Iterate selections
    for sel in selections:
        target_table = str(sel.get("source_table", "")).strip().upper()
        target_id = str(sel.get("source_id"))
        indices_str = str(sel.get("selected_indices", ""))

        try:
            indices = [
                int(i.strip()) for i in indices_str.split(",")
                if i.strip().isdigit()
            ]
        except Exception:
            indices = []

        # Find matching node in memory
        matching_node = None
        for node in LATEST_NEIGHBOR_RESULT:
            node_table = str(node.get("source_table", "")).strip().upper()
            node_id = str(node.get("source_id"))

            if node_table == target_table and node_id == target_id:
                matching_node = node
                break

        if matching_node:
            all_neighbors = matching_node.get("neighbor_results", [])
            selected_neighbors = []

            for idx in indices:
                if 0 <= idx < len(all_neighbors):
                    nb = all_neighbors[idx]
                    selected_neighbors.append(nb)

                    next_table = nb.get("target_table")
                    next_id = nb.get("target_id")

                    if next_table and next_id:
                        # --- DIRECT ADD (NO CHECKS) ---
                        cache_key = make_key(next_table, next_id)
                        if cache_key not in VISITED_NODES:
                            next_unvisited_nodes.append({
                                "table_name": next_table,
                                "node_id": next_id
                            })
                        # ------------------------------

            # Store the user's selection
            entry = {
                "source_table": matching_node.get("source_table"),
                "source_id": target_id,
                "source_row_data": matching_node.get("source_row_data"),
                "selected_neighbors": selected_neighbors
            }
            GLOBAL_SELECTED_RESULTS.append(entry)
            count_processed += 1

    print(
        f"✅ Processed {count_processed} source items. Added {len(next_unvisited_nodes)} nodes to next iteration (Raw add)."
    )

    # 3. Update Globals
    CURRENT_ITERATION_NODES = next_unvisited_nodes
    LATEST_NEIGHBOR_RESULT = []

    return {
        "status": "success",
        "processed_count": count_processed,
        "global_total_stored": len(GLOBAL_SELECTED_RESULTS),
        "next_unvisited_nodes": next_unvisited_nodes
    }


@app.get("/get_selected_results")
def api_get_selected_results():
    """
    Returns the accumulated list of all selected results from the user.
    """
    global GLOBAL_SELECTED_RESULTS
    print("yessss", GLOBAL_SELECTED_RESULTS)
    return {
        "status": "success",
        "count": len(GLOBAL_SELECTED_RESULTS),
        "data": GLOBAL_SELECTED_RESULTS
    }


class EntityCreate(BaseModel):
    result: Any


@app.post("/entity")
def create_entity(payload: EntityCreate):
    # pool = APP_STATE.get("pool")
    # with pool.acquire() as conn:
    # entry_point(conn,payload.result)
    print(payload.result)
    return {"status": "success"}


@app.get("/check")
def checker():
    global CURRENT_ITERATION_NODES
    print("/check",len(CURRENT_ITERATION_NODES))
    # Python's ternary: [value_if_true] if [condition] else [value_if_false]
    return {"status": "true" if len(CURRENT_ITERATION_NODES) > 0 else "false"}


if __name__ == "__main__":
    print("started")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
