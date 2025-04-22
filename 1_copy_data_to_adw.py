import oci
import oracledb
import pandas as pd
import os
import numpy as np
import time
import random
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64


# === CONFIG ===
bucket_name = "cost_and_usage_reports" # change it based on your environment
namespace = "ociateam" # change it based on your environment
adw_user = "admin" # change it based on your environment
dsn = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-chicago-1.oraclecloud.com))(connect_data=(service_name=g1e6630826ad105_uwaehhskrm1qay4v_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"


local_folder = "temp_csvs"
os.makedirs(local_folder, exist_ok=True)

# === OCI SETUP ===
signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
secrets_client = oci.secrets.SecretsClient(config={}, signer=signer)
vault_client = oci.vault.VaultsClient(config={}, signer=signer)
secret_ocid = "ocid1.vaultsecret.oc1.iad.amaaaaaac3adhhqacfyyvmkejvczkklmmex7xirxyc3hyynboi72xzok4ica" # Replace with your actual OCID of the secret
base64_secret = secrets_client.get_secret_bundle(secret_ocid).data.secret_bundle_content.content

decoded_bytes = base64.b64decode(base64_secret)
adw_password = decoded_bytes.decode("utf-8")
# === PAGINATED OBJECT LISTING ===
def list_all_objects(namespace, bucket_name):
    all_objects = []
    next_start_with = None

    while True:
        response = object_storage.list_objects(
            namespace_name=namespace,
            bucket_name=bucket_name,
            start=next_start_with
        )
        all_objects.extend(response.data.objects)

        next_start_with = response.data.next_start_with
        if not next_start_with:
            break

    return all_objects

# Fetch all objects
objects = list_all_objects(namespace, bucket_name)

start_date = datetime.utcnow() - timedelta(days=1)
start_date = datetime(start_date.year, start_date.month, start_date.day)  
csv_files = []

for obj in objects:
    name = obj.name
    if name.endswith(".csv"):
        try:
            parts = name.split("/")
            if len(parts) >= 4:
                y, m, d = int(parts[1]), int(parts[2]), int(parts[3])                
                file_date = datetime(y, m, d)
                if file_date >= start_date:
                    csv_files.append(name)
        except Exception as e:
            print(f"Skipping '{name}' due to parsing error: {e}")
print(f"csv_files are {csv_files}")
# === GLOBAL CONNECTION POOL (created once) ===
#delete code
pool = oracledb.create_pool(
    user=adw_user,
    password=adw_password,
    dsn=dsn,
    min=1,
    max=5,
    increment=1,
    timeout=60,
    getmode=oracledb.POOL_GETMODE_WAIT
)

# === PROCESS ONE FILE ===
def process_file(file):
    print(f"[Thread] Processing: {file}")
    local_path = os.path.join(local_folder, os.path.basename(file))

    try:
        # Download the file from OCI Object Storage
        obj = object_storage.get_object(namespace, bucket_name, file)
        with open(local_path, "wb") as f:
            f.write(obj.data.content)

        # Load and clean CSV
        df = pd.read_csv(local_path, low_memory=False)
        drop_cols = ['Tags', 'oci_BackReferenceNumber']
        for col in drop_cols:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        for col in ["BillingPeriodStart", "BillingPeriodEnd", "ChargePeriodStart", "ChargePeriodEnd"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        numeric_columns = df.select_dtypes(include=["float", "int"]).columns.tolist()
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.astype({col: "float64" for col in df.select_dtypes(include="int64").columns})
        df.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)

        insert_columns = ",".join(df.columns)
        placeholders = ",".join([f":{i+1}" for i in range(len(df.columns))])
        insert_sql = f"INSERT INTO oci_cost_data ({insert_columns}) VALUES ({placeholders})"
        rows = df.values.tolist()

        # Retry connection logic
        conn = None
        for attempt in range(3):
            try:
                conn = pool.acquire()
                break
            except Exception as e:
                print(f"Failed to acquire DB connection (attempt {attempt+1}): {e}")
                time.sleep(2 ** attempt + random.uniform(0, 1))
        if not conn:
            raise Exception("Could not acquire DB connection after 3 retries.")

        with conn.cursor() as cursor:
            for i, row in enumerate(rows):
                try:
                    cursor.execute(insert_sql, row)
                except Exception as e:
                    print(f"\n Row {i} in {file} failed: {e}")
                    for col, val in zip(df.columns, row):
                        print(f"   {col}: {val} ({type(val).__name__})")
                    raise
        conn.commit()
        print(f"Loaded {len(rows)} rows from {file}")

        # Clean up local file after successful processing
        try:
            os.remove(local_path)
            print(f"Deleted local file: {local_path}")
        except Exception as cleanup_err:
            print(f"Could not delete file {local_path}: {cleanup_err}")

    except Exception as e:
        print(f"\n Error processing file '{file}': {e}")

# === MAIN EXECUTION ===
max_workers = 5  # Adjust based on DB capacity
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(process_file, f): f for f in csv_files}
    for future in as_completed(futures):
        file = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Uncaught error in thread for file '{file}': {e}")
