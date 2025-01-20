GERMAN_PYTHON_ENCODING = "iso-8859-1"
PRODUCT_DETAILS_COLUMNS = ["product_id", "description", "product_group"]

PRODUCT_DETAILS_SCHEMA = {
    "product_id": str,
    "description": str,
    "product_group": str,
}

PICK_DATA_COLUMNS = [
    "product_id",
    "warehouse_section",
    "origin",
    "order_number",
    "position_in_order",
    "pick_volume",
    "quantity_unit",
    "date",
]

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


WAREHOUSE_SECTION_COLUMNS = ["abbreviation", "description", "group", "pick_reference"]

WAREHOUSE_SECTION_SCHEMA = {i: str for i in WAREHOUSE_SECTION_COLUMNS}

BASE_PATH = "/Users/aprajita/Desktop/APRAJITA_DA_PROJ/obeta-group-5/data"
