GERMAN_PYTHON_ENCODING = "iso-8859-1"
PRODUCT_DETAILS_COLUMNS = [
    "product_id", "description", "product_group"
]

PRODUCT_DETAILS_SCHEMA = {"product_id": str, "description": str, "product_group": "category"}

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
