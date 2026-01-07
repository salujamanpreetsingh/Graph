import os
import sys
import base64
import zipfile
import tempfile
import shutil
import traceback
import oracledb

# --- CONFIGURATION ---
DB_USER = os.environ.get("DB_USER", "ADMIN")
DB_PASS = os.environ.get("DB_PASS", "")
DB_CONNECT = os.environ.get("DB_CONNECT", "")
WALLET_B64 = os.environ.get("WALLET_B64")
WALLET_PASSWORD = os.environ.get("WALLET_PASSWORD")

POOL = None

def ensure_wallet_dir():
    """
    Decodes the wallet and finds the directory with tnsnames.ora
    """
    if not WALLET_B64:
        # If no wallet secret, return None (allows Thin mode if DSN is clean)
        return None

    tmpdir = os.path.join(tempfile.gettempdir(), "oracle_wallet_replit")
    extract_root = os.path.join(tmpdir, "extracted")

    # Clean & Re-extract
    if os.path.exists(extract_root): 
        shutil.rmtree(extract_root)
    os.makedirs(extract_root, exist_ok=True)

    zip_path = os.path.join(tmpdir, "wallet.zip")
    try:
        # Handle padding
        b64_clean = WALLET_B64.strip()
        missing_padding = len(b64_clean) % 4
        if missing_padding:
            b64_clean += '=' * (4 - missing_padding)

        with open(zip_path, "wb") as f:
            f.write(base64.b64decode(b64_clean))

        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_root)

        # Recursive search for tnsnames.ora
        for root, dirs, files in os.walk(extract_root):
            if "tnsnames.ora" in files:
                print(f"✅ Found tnsnames.ora at: {root}")
                return root

        print("⚠️ Wallet extracted, but tnsnames.ora not found.")
        return None

    except Exception as e:
        print(f"❌ Wallet Extraction Error: {e}")
        return None

# --- YOUR WORKING CODE ---
def create_pool():
    global POOL
    if POOL:
        return POOL

    if not (DB_USER and DB_PASS and DB_CONNECT):
        raise RuntimeError("DB_USER/DB_PASS/DB_CONNECT must be set in env")

    # Ensure wallet dir exists and get path
    wallet_dir = None
    try:
        wallet_dir = ensure_wallet_dir()
    except Exception as e:
        print("⚠️ Wallet ensure failed:", e)
        wallet_dir = None

    try:
        # Provide wallet configuration if available
        pool_args = dict(
            user=DB_USER,
            password=DB_PASS,
            dsn=DB_CONNECT,
        )
        if wallet_dir:
            # oracledb supports config_dir / wallet_location params
            pool_args.update(
                dict(config_dir=wallet_dir, wallet_location=wallet_dir))
            if WALLET_PASSWORD:
                pool_args.update(dict(wallet_password=WALLET_PASSWORD))

        POOL = oracledb.create_pool(**pool_args)
        print("✅ Oracle pool created")
        return POOL
    except Exception as e:
        print("❌ create_pool error:", e)
        traceback.print_exc()
        raise