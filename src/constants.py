"""
This file defines constants, schemas, and configurations used across the ETL pipeline.
These include column definitions, data schemas, encoding settings, and base file paths.
"""
# Encoding for reading/writing files
GERMAN_PYTHON_ENCODING = "iso-8859-1"

# Columns for the product details dataset
PRODUCT_DETAILS_COLUMNS = ["product_id", "description", "product_group"]

# Schema for product details data (used for validation)
PRODUCT_DETAILS_SCHEMA = {
    "product_id": str,  # Unique product identifier
    "description": str,  # Product description
    "product_group": str,  # Product category or group
}

# Columns for the pick data dataset
PICK_DATA_COLUMNS = [
    "product_id",  # Unique product identifier
    "warehouse_section",  # Section of the warehouse
    "origin",  # Order origin: store (46) or customer (48)
    "order_number",  # Order number
    "position_in_order",  # Position of the product in the order
    "pick_volume",  # Quantity of the product picked
    "quantity_unit",  # Unit of measurement for the picked product
    "date",  # Timestamp of the pick operation
]


# Schema for pick data (used for validation)
PICK_DATA_SCHEMA = {
    "product_id": str,
    "warehouse_section": str,
    "origin": int,
    "order_number": str,
    "position_in_order": str,
    "pick_volume": int,
    "quantity_unit": str,
    "date": str,
}

# Columns for the warehouse sections dataset
WAREHOUSE_SECTION_COLUMNS = ["abbreviation", "description", "group", "pick_reference"]

# Schema for warehouse sections data (used for validation)
WAREHOUSE_SECTION_SCHEMA = {i: str for i in WAREHOUSE_SECTION_COLUMNS}

# Base path for source and processed data
BASE_PATH = "/Users/aprajita/Desktop/APRAJITA_DA_PROJ/obeta-group-5/data"
