from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    TimestamptzType,
)

catalog = load_catalog(
    "lakehouse_catalog",
    **{
        "type": "rest",
        "uri": "http://localhost:8181/api/catalog",
        "credential": "root:s3cr3t",
        "scope": "PRINCIPAL_ROLE:ALL",
        "warehouse": "lakehouse_catalog",
        "s3.endpoint": "http://localhost:9000",
        "s3.region": "us-east-1",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
        "s3.path-style-access": "true",
    },
)

schema = Schema(
    NestedField(1, "did",          StringType(),     required=False),
    NestedField(2, "time_us",      LongType(),       required=False),
    NestedField(3, "event_time",   TimestamptzType(), required=False),
    NestedField(4, "text",         StringType(),     required=False),
    NestedField(5, "primary_lang", StringType(),     required=False),
    NestedField(6, "created_at",   StringType(),     required=False),
)

try:
    table = catalog.create_table("bsky.posts", schema)
    print(f"✓ Table created: {table.identifier()}")
    print(f"  location: {table.location()}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Table already exists — good to go")
    else:
        raise