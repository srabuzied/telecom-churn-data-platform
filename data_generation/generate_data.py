import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# CONFIG
# ==========================================

TARGET_ROWS = 1_000_000

CLIENT_FILE = "Client.csv"
RECORD_FILE = "Record.csv"

CLIENT_OUTPUT = "Client_1M.csv"
RECORD_OUTPUT = "Record_1M.csv"

# ==========================================
# LOAD DATA
# ==========================================

print("Loading datasets...")

client = pd.read_csv(CLIENT_FILE)
record = pd.read_csv(RECORD_FILE)

print(f"Original Client Rows : {len(client)}")
print(f"Original Record Rows : {len(record)}")

# ==========================================
# CALCULATE REQUIRED ROWS
# ==========================================

rows_to_generate = TARGET_ROWS - len(client)

print(f"\nNeed to generate: {rows_to_generate:,} rows")

# ==========================================
# CLIENT MODEL
# ==========================================

print("\nTraining Client Synthesizer...")

client_meta = SingleTableMetadata()
client_meta.detect_from_dataframe(client)

client_meta.update_column(
    column_name="Customer_ID",
    sdtype="id"
)

client_synth = GaussianCopulaSynthesizer(client_meta)

client_synth.fit(client)

print("Generating Client synthetic rows...")

client_generated = client_synth.sample(
    num_rows=rows_to_generate
)

# ==========================================
# CREATE NEW IDS
# ==========================================

start_id = client["Customer_ID"].max() + 1

new_ids = range(
    start_id,
    start_id + rows_to_generate
)

client_generated["Customer_ID"] = list(new_ids)

# ==========================================
# RECORD MODEL
# ==========================================

print("\nTraining Record Synthesizer...")

record_meta = SingleTableMetadata()
record_meta.detect_from_dataframe(record)

record_meta.update_column(
    column_name="Customer_ID",
    sdtype="id"
)

record_synth = GaussianCopulaSynthesizer(record_meta)

record_synth.fit(record)

print("Generating Record synthetic rows...")

record_generated = record_synth.sample(
    num_rows=rows_to_generate
)

# IMPORTANT:
# use exactly same Customer_ID values

record_generated["Customer_ID"] = client_generated["Customer_ID"].values

# ==========================================
# COMBINE ORIGINAL + GENERATED
# ==========================================

print("\nCombining datasets...")

client_final = pd.concat(
    [client, client_generated],
    ignore_index=True
)

record_final = pd.concat(
    [record, record_generated],
    ignore_index=True
)

# ==========================================
# SAVE FILES
# ==========================================

print("\nSaving files...")

client_final.to_csv(
    CLIENT_OUTPUT,
    index=False
)

record_final.to_csv(
    RECORD_OUTPUT,
    index=False
)

# ==========================================
# VALIDATION
# ==========================================

print("\nValidation")

print("-" * 50)

print("Client Final Shape:")
print(client_final.shape)

print("\nRecord Final Shape:")
print(record_final.shape)

print("\nCustomer_ID Check")

client_ids = set(client_final["Customer_ID"])
record_ids = set(record_final["Customer_ID"])

print(
    "IDs Match:",
    client_ids == record_ids
)

print("\nFiles Saved Successfully")

print(f"Client -> {CLIENT_OUTPUT}")
print(f"Record -> {RECORD_OUTPUT}")

print("\nDone!")