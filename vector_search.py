import json
import os
import traceback
import array
from typing import List, Dict
from huggingface_hub import InferenceClient

# CONFIG
TOP_K = 8
HF_TOKEN = os.environ.get("HF_TOKEN")
MODEL_ID = "sentence-transformers/all-mpnet-base-v2"

PG_TABLES_TO_SEARCH = [
    ("ENTITIES", "ENTITY_ID", "EMBEDDING"),
    ("SUPPLIERS", "SUPPLIER_ID", "EMBEDDING"),
    ("COMPONENTS", "COMPONENT_ID", "EMBEDDING"),
    ("PRODUCTS", "PRODUCT_ID", "EMBEDDING"),
    ("CUSTOMERS", "CUSTOMER_ID", "EMBEDDING"),
    ("PURCHASE_ORDERS", "PO_ID", "EMBEDDING"),
    ("SALES_ORDERS", "ORDER_ID", "EMBEDDING"),
    ("CONTRACTS", "CONTRACT_ID", "EMBEDDING"),
    ("INVOICES", "INVOICE_ID", "EMBEDDING"),
    ("PAYMENTS", "PAYMENT_ID", "EMBEDDING"),
]


def get_embedding_online(text: str):
    """Uses the official HF Client to get embeddings safely"""
    if not HF_TOKEN:
        print("❌ Error: HF_TOKEN secret is missing.")
        return None

    try:
        client = InferenceClient(token=HF_TOKEN)
        # feature_extraction is the API call for embeddings
        embedding = client.feature_extraction(text, model=MODEL_ID)
        return embedding.tolist()
    except Exception as e:
        print(f"❌ HF Client Error: {e}")
        return None


# def run_vector_search(conn, query_string: str) -> List[Dict]:
#     print(f"🔍 Searching for: '{query_string}'")

#     try:
#         # 1. Get Embedding
#         embedding_list = get_embedding_online(query_string)

#         if not embedding_list:
#             return [{"error": "Failed to generate embedding"}]

#         # Flatten if it's a list of lists (common with HF API)
#         if isinstance(embedding_list,
#                       list) and len(embedding_list) > 0 and isinstance(
#                           embedding_list[0], list):
#             embedding_list = embedding_list[0]

#         # 2. CONVERT TO NATIVE ARRAY (THE FIX: 'f' instead of 'd')
#         # 'f' = Float (32-bit), matching Oracle VECTOR(FLOAT32)
#         # 'd' = Double (64-bit), which caused your error
#         vector_bind = array.array('f', [float(x) for x in embedding_list])

#         # 3. SQL Query
#         select_parts = []
#         for (table_name, pk_col, emb_col) in PG_TABLES_TO_SEARCH:
#             part = f"""
#             SELECT '{table_name}' AS table_name,
#                    {pk_col} AS pk_value,
#                    {emb_col} <-> :embedding AS dist
#             FROM {table_name}
#             WHERE {emb_col} IS NOT NULL
#             """
#             select_parts.append(part.strip())

#         union_sql = "\nUNION ALL\n".join(select_parts)

#         final_sql = f"""SELECT table_name, pk_value, dist FROM ({union_sql}) ORDER BY dist ASC FETCH FIRST {TOP_K} ROWS ONLY"""

#         results = []
#         with conn.cursor() as cur:
#             cur.execute(final_sql, {"embedding": vector_bind})

#             for r in cur.fetchall():
#                 results.append({
#                     "table_name":
#                     str(r[0]),
#                     "node_id":
#                     int(r[1]) if hasattr(r[1], "imag") else str(r[1]),
#                 })

#         print(f"✅ Found {len(results)} matches.")
#         return results

#     except Exception as e:
#         print(f"❌ Vector Search Logic Error: {e}")
#         traceback.print_exc()
#         return []


def run_vector_search(conn,
                    query_string: str,
                    threshold: float = 0.9) -> List[Dict]:
    # Note: For Cosine Distance, 0.0 is an exact match, 1.0 is orthogonal, 2.0 is opposite.
    # A common threshold for 'similar' content is between 0.1 and 0.4.

    print(f"🔍 Cosine Search: '{query_string}' (Threshold: {threshold})")

    try:
        embedding_list = get_embedding_online(query_string)
        if not embedding_list: return []

        if isinstance(embedding_list,
                    list) and len(embedding_list) > 0 and isinstance(
                        embedding_list[0], list):
            embedding_list = embedding_list[0]

        vector_bind = array.array('f', [float(x) for x in embedding_list])

        select_parts = []
        for (table_name, pk_col, emb_col) in PG_TABLES_TO_SEARCH:
            # CHANGE: Used <=> for Cosine Distance
            part = f"""
            SELECT '{table_name}' AS table_name, 
                {pk_col} AS pk_value, 
                {emb_col} <=> :embedding AS dist 
            FROM {table_name} 
            WHERE {emb_col} IS NOT NULL
            """
            select_parts.append(part.strip())

        union_sql = "\nUNION ALL\n".join(select_parts)

        # Filter results where distance is less than or equal to threshold
        final_sql = f"""
            SELECT table_name, pk_value, dist 
            FROM ({union_sql}) 
            WHERE dist <= :threshold 
            ORDER BY dist ASC
        """

        results = []
        with conn.cursor() as cur:
            cur.execute(final_sql, {
                "embedding": vector_bind,
                "threshold": threshold
            })

            for r in cur.fetchall():
                results.append({
                    "table_name":
                    str(r[0]),
                    "node_id":
                    int(r[1]) if hasattr(r[1], "imag") else str(r[1]),
                    "cosine_distance":
                    round(float(r[2]), 4)
                })

        return results

    except Exception as e:
        print(f"❌ Error: {e}")
        return []
