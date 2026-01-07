import json
import traceback
import oracledb
from typing import List, Dict, Any
from collections import defaultdict
from decimal import Decimal
from datetime import date, timedelta

# ==========================================
# 1. CONFIGURATION & MAPPINGS
# ==========================================

mapping = {
    "suppliers": "supplier",
    "components": "component",
    "products": "product",
    "customers": "customer",
    "purchase_orders": "purchase_order",
    "sales_orders": "sales_order",
    "contracts": "contract",
    "invoices": "invoice",
    "payments": "payment",
    "entities": "unstructured_entity",
}

TABLE_TO_PK_COLUMN_MAP = {
    "ENTITIES": "ENTITY_ID",
    "SUPPLIERS": "SUPPLIER_ID",
    "COMPONENTS": "COMPONENT_ID",
    "PRODUCTS": "PRODUCT_ID",
    "CUSTOMERS": "CUSTOMER_ID",
    "PURCHASE_ORDERS": "PO_ID",
    "SALES_ORDERS": "ORDER_ID",
    "CONTRACTS": "CONTRACT_ID",
    "INVOICES": "INVOICE_ID",
    "PAYMENTS": "PAYMENT_ID",
}

PK_LOOKUP = {
    table.upper(): pk.upper()
    for table, pk in TABLE_TO_PK_COLUMN_MAP.items()
}

graph_edge_label_mapping = {
    "RELATIONS": "KG_RELATION",
    "SUPPLIERS_MAPPINGS": "MAPS_TO_SUPPLIER",
    "COMPONENTS_MAPPINGS": "MAPS_TO_COMPONENT",
    "PRODUCTS_MAPPINGS": "MAPS_TO_PRODUCT",
    "CUSTOMERS_MAPPINGS": "MAPS_TO_CUSTOMER",
    "PURCHASE_ORDERS_MAPPINGS": "MAPS_TO_PO",
    "SALES_ORDERS_MAPPINGS": "MAPS_TO_SO",
    "CONTRACTS_MAPPINGS": "MAPS_TO_CONTRACT",
    "INVOICES_MAPPINGS": "MAPS_TO_INVOICE",
    "PAYMENTS_MAPPINGS": "MAPS_TO_PAYMENT",
    "BILL_OF_MATERIALS": "USES",
    "COMPONENT_SUPPLIER": "SUPPLIED_BY",
    "CONTRACT_SUPPLIER": "GOVERNS_SUPPLIER",
    "PO_SUPPLIER": "ORDERS_FROM",
    "PO_CONTRACT": "UNDER_CONTRACT",
    "PO_LINE_ITEMS": "ORDERS_COMPONENT",
    "SO_CUSTOMER": "ORDERED_BY",
    "SO_PRODUCT": "REQUESTS_PRODUCT",
    "INVOICE_SUPPLIER": "BILLS_FROM_SUPPLIER",
    "INVOICE_CUSTOMER": "BILLS_TO_CUSTOMER",
    "INVOICE_PO": "FOR_PURCHASE_ORDER",
    "INVOICE_SO": "FOR_SALES_ORDER",
    "PAYMENT_INVOICE": "PAYS_INVOICE",
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================


def safe_read_clob(val):
    if val is None: return None
    return val.read() if hasattr(val, "read") else val


def get_selectable_columns(conn, table_name, owner):
    """
    Get columns excluding heavy LOBs/Vectors.
    """
    sql = """
        SELECT column_name, data_type
        FROM ALL_TAB_COLUMNS
        WHERE owner = :owner AND table_name = :t
        ORDER BY column_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, owner=owner, t=table_name.upper())
        cols = []
        for col_name, data_type in cur.fetchall():
            col_upper = col_name.upper()
            if data_type and data_type.upper() == "VECTOR": continue
            if "EMBEDDING" in col_upper or "CREATED_DATE" in col_upper or "UUID" in col_upper or "LAST_UPDATED" in col_upper:
                continue
            cols.append(col_name)
        return cols


def fetch_rows_for_table(conn, table_name, pk_col, pk_values, columns, owner):
    """
    Batch fetch row details.
    """
    if not pk_values: return {}

    binds = {f"b{i}": v for i, v in enumerate(pk_values)}
    bind_clause = ", ".join(f":{k}" for k in binds.keys())

    clean_cols = [c for c in columns if c.upper() != pk_col.upper()]
    select_cols = f'"{pk_col}", ' + ", ".join(f'"{c}"' for c in clean_cols)

    sql = f'SELECT {select_cols} FROM "{owner}"."{table_name.upper()}" WHERE "{pk_col}" IN ({bind_clause})'

    out = {}
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        desc = [d[0] for d in cur.description]
        for row in cur.fetchall():
            row_dict = dict(zip(desc, row))
            cleaned = {k: v for k, v in row_dict.items() if v is not None}
            # Handle Decimals/Dates for JSON serialization
            for k, v in cleaned.items():
                if isinstance(v, Decimal): cleaned[k] = float(v)
                if isinstance(v, (date, timedelta)):
                    cleaned[k] = str(v.isoformat())

            pk_val = cleaned.get(pk_col)
            out[pk_val] = cleaned
    return out


def attach_destination_rows(neighbors, conn, owner):
    """
    Fetches full row data for every neighbor found in the graph.
    """
    table_to_pks = defaultdict(set)
    for e in neighbors:
        t = e.get("dest_vertex_table")
        v = e.get("dest_key_value")
        if t and v is not None:
            table_to_pks[t.upper()].add(v)

    table_to_rows = {}
    for dest_table, pk_vals in table_to_pks.items():
        pk_col = PK_LOOKUP.get(dest_table.upper())
        if not pk_col: continue

        cols = get_selectable_columns(conn, dest_table, owner)
        if cols:
            rows = fetch_rows_for_table(conn, dest_table, pk_col,
                                        list(pk_vals), cols, owner)
            table_to_rows[dest_table] = rows

    for e in neighbors:
        dest_table = (e.get("dest_vertex_table") or "").upper()
        dest_pk_val = e.get("dest_key_value")
        rows_map = table_to_rows.get(dest_table, {})
        dest_row = rows_map.get(dest_pk_val, {})

        # Rename PK to 'id' for cleaner JSON
        pk_col = PK_LOOKUP.get(dest_table.upper())
        if pk_col and pk_col in dest_row:
            dest_row[pk_col] = dest_row.pop(pk_col)

        e["dest_row"] = dest_row


# ==========================================
# 3. MAIN LOGIC (One-Hop)
# ==========================================


def get_neighbors_logic(conn, table_name: str, pk_value: Any, owner: str,
                        graph_name: str):
    """
    Executes the One-Hop logic and returns the DICTIONARY result.
    """
    print(f"🕸️  Graph Search: {table_name} ID={pk_value}")

    table_upper = table_name.upper()
    vertex_label = mapping.get(table_name.lower())
    pk_attr = PK_LOOKUP.get(table_upper)

    if not vertex_label:
        return {"error": f"No mapping for table {table_name}"}

    # 1. Fetch Source Node Data
    source_row_data = {}
    try:
        source_cols = get_selectable_columns(conn, table_name, owner)
        source_results = fetch_rows_for_table(conn, table_name, pk_attr,
                                            [pk_value], source_cols, owner)
        # robustly resolve source_row_data even when pk_value types differ (str vs int)
        source_row_data = None

        # 1) direct look-up
        if pk_value in source_results:
            source_row_data = source_results[pk_value]
        else:
            # 2) try integer conversion if pk_value looks like an int
            try:
                ik = int(pk_value)
                if ik in source_results:
                    source_row_data = source_results[ik]
            except Exception:
                pass

        # 3) fallback: compare stringified keys (covers uuid/str/int mismatches)
        if source_row_data is None:
            for k, v in source_results.items():
                if str(k) == str(pk_value):
                    source_row_data = v
                    break

        # final fallback
        if source_row_data is None:
            source_row_data = {}

    except Exception as e:
        print(f"⚠️ Source Fetch Error: {e}")
        source_row_data = {"id": pk_value}

    # 2. Run Graph Query (YOUR EXACT SQL)
    sql_query = f"""
    SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
            'edge_table' VALUE t.edge_name.ELEM_TABLE,
            'dest_vertex_table' VALUE t.destination_vertex.ELEM_TABLE,
            'dest_key_value' VALUE t.destination_vertex.KEY_VALUE,
            'direction' VALUE t.edge_direction,
            'relation' VALUE relation
        )
        RETURNING CLOB
    )
    FROM GRAPH_TABLE(
        "{owner}"."{graph_name}"
        MATCH (v IS {vertex_label})-[e]-(n)
        WHERE v.{pk_attr} = :pk
        COLUMNS (
            edge_id(e) AS edge_name,
            e.RELATION_TYPE as relation,
            vertex_id(n) AS destination_vertex,
            CASE
                WHEN v IS SOURCE OF e THEN 'OUTGOING'
                ELSE 'INCOMING'
            END AS edge_direction                
        )
    ) t
    """

    neighbors = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query, pk=pk_value)
            row = cur.fetchone()
            s = safe_read_clob(row[0]) if row else None
            neighbors = json.loads(s) if s else []
    except Exception as e:
        print(f"❌ Graph Query Error: {e}")
        return {"error": str(e)}

    # 3. Clean & Attach Data
    for entry in neighbors:
        # Normalize ID
        dv = entry.get("dest_key_value")
        if isinstance(dv, dict) and len(dv) == 1:
            entry["dest_key_value"] = list(dv.values())[0]

        # Add Label
        edge_table_key = (entry.get("edge_table") or "").upper()
        edge_label = graph_edge_label_mapping.get(edge_table_key)
        entry[
            "relationship"] = edge_label if edge_label else f"UNKNOWN_{edge_table_key}"

    # Attach Full Row Data
    try:
        attach_destination_rows(neighbors, conn, owner)
    except Exception as e:
        print(f"⚠️ Attach Rows Error: {e}")

    # 4. Filter and Format Output
    keys_to_keep = [
        "relationship", "direction", "dest_row", "relation",
        "dest_vertex_table"
    ]
    filtered_neighbors = []

    for entry in neighbors:
        # Keep only required keys
        filtered_entry = {
            k: entry[k]
            for k in keys_to_keep if k in entry and entry[k] is not None
        }
        # Add index for the LLM selection
        filtered_entry["target_table"] = entry.get("dest_vertex_table")
        filtered_entry["target_id"] = entry.get("dest_key_value")
        filtered_neighbors.append(filtered_entry)

    # Return Result
    return {
        "source_table": table_name,
        "source_row_data": source_row_data,
        "neighbor_results": filtered_neighbors
    }
