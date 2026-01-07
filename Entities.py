"""
db_mapper.py

This script loads Knowledge Graph triplets from a JSON file, 
inserts entities (with name-based vector embeddings, type, and provenance) 
and relations into an Oracle database. 

It performs a global vector similarity search and inserts results into 
**DECENTRALIZED MAPPING TABLES** (e.g., SUPPLIERS_MAPPINGS) by taking the 
TOP_K=2 closest matches, regardless of the similarity distance.
"""

import json
import traceback
from typing import List, Dict, Any, Tuple, Optional
import os
from huggingface_hub import InferenceClient

TOP_K = 2  # <-- Global limit: now strictly enforced
HF_TOKEN = os.environ.get("HF_TOKEN")
MODEL_ID = "sentence-transformers/all-mpnet-base-v2"

client = InferenceClient(token=HF_TOKEN)


def get_embedding(text: str):
    """Uses HF Inference API for feature extraction"""
    try:
        embedding = client.feature_extraction(text, model=MODEL_ID)
        return embedding.tolist()
    except Exception as e:
        print(f"❌ HF Client Error: {e}")
        return None


PG_TABLES_TO_SEARCH = [
    ("SUPPLIERS", "SUPPLIER_ID", "UUID", "EMBEDDING"),
    ("COMPONENTS", "COMPONENT_ID", "UUID", "EMBEDDING"),
    ("PRODUCTS", "PRODUCT_ID", "UUID", "EMBEDDING"),
    ("CUSTOMERS", "CUSTOMER_ID", "UUID", "EMBEDDING"),
    ("PURCHASE_ORDERS", "PO_ID", "UUID", "EMBEDDING"),
    ("SALES_ORDERS", "ORDER_ID", "UUID", "EMBEDDING"),
    ("CONTRACTS", "CONTRACT_ID", "UUID", "EMBEDDING"),
    ("INVOICES", "INVOICE_ID", "UUID", "EMBEDDING"),
    ("PAYMENTS", "PAYMENT_ID", "UUID", "EMBEDDING"),
]

# Map to easily find the PK name for the dynamic decentralized INSERT
TABLE_PK_MAP = {t[0]: t[1] for t in PG_TABLES_TO_SEARCH}


def load_triplets(data):
    """Loads and normalizes triplets from the JSON file."""
    # with open(path, "r", encoding="utf-8") as fh:
    #     data = json.load(fh)
    data = json.loads(data)
    print(data)
    if not isinstance(data, list):
        raise ValueError("Triples JSON must be a list.")
    normalized = []
    for i, obj in enumerate(data, start=1):
        # Handle various key spellings and ensure non-empty subject/predicate/object
        subj = obj.get("subject") or obj.get("Subject") or ""
        subj_type = obj.get("subject_type") or obj.get("subjectType") or None
        pred = obj.get("predicate") or obj.get("relation") or obj.get(
            "rel") or ""
        objv = obj.get("object") or obj.get("Object") or ""
        obj_type = obj.get("object_type") or obj.get("objectType") or None
        prov = obj.get("provenance") or None
        if not subj or not pred or not objv:
            print(f"[WARN] Skipping invalid triple #{i}: {obj}")
            continue
        normalized.append({
            "subject":
            str(subj).strip(),
            "subject_type": (str(subj_type).strip() if subj_type else None),
            "predicate":
            str(pred).strip(),
            "object":
            str(objv).strip(),
            "object_type": (str(obj_type).strip() if obj_type else None),
            "provenance":
            prov
        })
    return normalized


# -------------------------
# Entities: upsert / insert
# -------------------------
def get_entity_by_name(conn, name: str, etype: str) -> Optional[int]:
    """Retrieves an existing entity_id based on name and type."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT entity_id FROM entities WHERE name = :name AND type = :typ",
            {
                "name": name,
                "typ": etype
            })
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close()


def insert_entity(conn,name: str,etype: str,provenance: Optional[str] = None) -> int:
    """Inserts a new entity, computing the embedding from its name."""
    cur = conn.cursor()
    try:
        embedding = json.dumps(get_embedding(name))
        # SQL computes embedding using ALL_MINILM_L12_V2 on the entity name
        insert_sql = """
        INSERT INTO entities (name, type, provenance, embedding)
        VALUES (:name, :typ, :prov,
                :embedding
        )
        RETURNING entity_id INTO :out_id
        """
        out_id = cur.var(int)
        cur.execute(
            insert_sql, {
                "name": name,
                "typ": etype,
                "prov": provenance,
                "out_id": out_id,
                "embedding": embedding
            })
        val = out_id.getvalue()
        return int(val[0]) if isinstance(val, (list, tuple)) else int(val)
    finally:
        cur.close()


def upsert_entity(conn, name: str, etype: Optional[str],provenance: Optional[str]) -> int:
    """Finds or creates an entity and returns its ID."""
    used_type = etype if etype else ("PERSON_OR_ORG" if any(
        c.isalpha() for c in name) else "UNKNOWN")
    existing = get_entity_by_name(conn, name, used_type)
    if existing:
        return existing
    return insert_entity(conn, name, used_type, provenance)


# -------------------------
# Relations
# -------------------------
def insert_relation(conn, source_id: int, target_id: int,
                    relation_type: str) -> int:
    """Inserts a new relation between two entities."""
    cur = conn.cursor()
    try:
        insert_sql = """
        INSERT INTO relations (source_id, target_id, relation_type)
        VALUES (:source_id, :target_id, :relation_type)
        RETURNING relation_id INTO :out_id
        """
        out_id = cur.var(int)
        cur.execute(
            insert_sql, {
                "source_id": source_id,
                "target_id": target_id,
                "relation_type": relation_type,
                "out_id": out_id
            })
        val = out_id.getvalue()
        return int(val[0]) if isinstance(val, (list, tuple)) else int(val)
    finally:
        cur.close()


# -------------------------
# Mappings (DECENTRALIZED INSERT)
# -------------------------
def insert_decentralized_mapping(conn, entity_id: int, source_table_name: str,source_pk_value: int) -> int:
    """
    Inserts a mapping into the table-specific mapping table (e.g., SUPPLIERS_MAPPINGS).
    """
    cur = conn.cursor()
    mapping_table = f"{source_table_name}_MAPPINGS"

    try:
        insert_sql = f"""
        INSERT INTO {mapping_table} (entity_id, source_row_id)
        VALUES (:entity_id, :source_row_id)
        RETURNING mapping_id INTO :out_id
        """
        out_id = cur.var(int)
        cur.execute(
            insert_sql, {
                "entity_id": entity_id,
                "source_row_id": source_pk_value,
                "out_id": out_id
            })
        val = out_id.getvalue()
        return int(val[0]) if isinstance(val, (list, tuple)) else int(val)
    finally:
        cur.close()


# -------------------------
# GLOBAL search (REVERTED TO TOP-K LIMIT)
# -------------------------
def find_top_overall(conn, tables: List[Tuple[str, str, str, str]],entity_name: str) -> List[Tuple[str, int, str, float]]:
    """
    Performs a global ANN search across all tables and returns the TOP_K (2)
    rows with the lowest distance (highest similarity).
    """
    # Use the global TOP_K limit defined in the config section
    global TOP_K

    if not tables:
        return []

    print(
        f"[INFO] Applying fixed limit: Fetching TOP {TOP_K} matches regardless of distance."
    )

    select_parts = []
    for (table_name, pk_col, uuid_col, emb_col) in tables:
        # Uses UNION ALL to search every table simultaneously
        part = f"""
        SELECT '{table_name}' AS table_name, {pk_col} AS pk,{pk_col} AS uuid, {emb_col} <-> :embedding AS dist
        FROM {table_name}
        WHERE {emb_col} IS NOT NULL 
        """ # 💥 Threshold condition removed
        select_parts.append(part.strip())

    union_sql = "\nUNION ALL\n".join(select_parts)

    # 💥 KEY CHANGE: Reintroducing FETCH FIRST {TOP_K} ROWS ONLY
    final_sql = f"""
    SELECT table_name, pk, uuid, dist
    FROM (
        {union_sql}
    )
    ORDER BY dist 
    FETCH FIRST {TOP_K} ROWS ONLY
    """

    cur = conn.cursor()
    try:
        cur.execute(final_sql,
                    {"embedding": json.dumps(get_embedding(entity_name))})
        rows = cur.fetchall()

        results: List[Tuple[str, int, str, float]] = []
        for r in rows:
            table_name = str(r[0])
            pk = int(r[1])
            source_row_id = r[2]  # This is the PK value from the source table
            try:
                dist = float(r[3])
            except Exception:
                dist = float(r[3].__float__()) if hasattr(r[3],"__float__") else 0.0
            results.append((table_name, pk, str(source_row_id), dist))
        return results
    finally:
        cur.close()


# -------------------------
# Main Database Pipeline (UPDATED for Decentralization)
# -------------------------
def run_db_pipeline(triples_path, conn):
    """Orchestrates the entity upsert, relation insert, and decentralized mapping."""
    try:
        triples = load_triplets(triples_path)
        print(f"\n[INFO] --------------------------------------------------")
        print(
            f"[INFO] Starting Database Insertion and Decentralized Vector Mapping"
        )
        print(f"[INFO] Loaded {len(triples)} triplets for processing.")

        # 1. Collect unique entities
        unique_entities: Dict[str, Dict[str, Any]] = {}
        for t in triples:
            s, o = t["subject"], t["object"]
            for entity_name, entity_type_key in [(s, "subject_type"),(o, "object_type")]:
                entity_type = t.get(entity_type_key)
                # provenance = t.get("provenance")
                provenance = "test_main.pdf"

                if entity_name not in unique_entities:
                    unique_entities[entity_name] = {
                        "name": entity_name,
                        "type": entity_type,
                        "provenance": provenance
                    }
                elif entity_type:
                    unique_entities[entity_name]["type"] = entity_type
                elif provenance:
                    unique_entities[entity_name]["provenance"] = provenance

        # 2. Upsert Entities
        print(f"[INFO] Upserting {len(unique_entities)} entities...")
        for name, meta in unique_entities.items():
            try:
                eid = upsert_entity(conn, name, meta.get("type"),
                                    meta.get("provenance"))
                unique_entities[name]["entity_id"] = eid
            except Exception as exc:
                print(
                    f"[ERROR] fatal upsert failure for '{name}'. Rolling back. {exc}"
                )
                raise

        # 3. Insert Relations
        print(f"[INFO] Inserting {len(triples)} relations...")
        for idx, t in enumerate(triples, start=1):
            sid = unique_entities[t["subject"]]["entity_id"]
            tid = unique_entities[t["object"]]["entity_id"]
            pred = t["predicate"]
            try:
                insert_relation(conn, sid, tid, pred)
            except Exception as exc:
                print(
                    f"[ERROR] insert relation failed for triple #{idx}. Rolling back. {exc}"
                )
                raise

        conn.commit()
        print("[INFO] Entities & relations committed to DB.")

        # 4. GLOBAL Decentralized Mapping
        print(
            f"[INFO] Finding TOP {TOP_K} matches for each entity and inserting into dedicated mapping tables..."
        )
        for name, meta in unique_entities.items():
            eid = meta["entity_id"]
            try:
                # Get the TOP K matches
                matches = find_top_overall(conn, PG_TABLES_TO_SEARCH, entity_name=name)
            except Exception as exc:
                print(
                    f"[ERROR] global ANN query failed for entity '{name}'. Rolling back. {exc}"
                )
                raise

            if not matches:
                print(f"  -> Entity '{name}' found 0 matches.")
                continue

            for table_name, pk, source_row_id, dist in matches:
                try:
                    # Insert into the table-specific mapping table
                    mid = insert_decentralized_mapping(conn, eid, table_name,int(source_row_id))
                    print(
                        f"  -> mapped: entity {eid} -> {table_name}_MAPPINGS (Row ID={source_row_id}) dist={dist:.6f}"
                    )
                except Exception as exc:
                    print(
                        f"[ERROR] insert mapping failed for entity {eid} -> {table_name}({source_row_id}). Rolling back. {exc}"
                    )
                    traceback.print_exc()
                    raise

        conn.commit()
        print("✅ All done. Decentralized mappings committed.")

    except Exception as e:
        print("❌ Fatal Error in DB Pipeline:", e)
        traceback.print_exc()
        try:
            conn.rollback()
        except Exception:
            pass


def entry_point(conn,TRIPLES_JSON_PATH):
    run_db_pipeline(TRIPLES_JSON_PATH, conn)
