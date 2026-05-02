# service/master_validation_service.py

def validate_master_data(payload: dict):

    product_code = payload.get("product_code")
    cust_code = payload.get("cust_code")
    container_id = payload.get("container_id")

    if not product_code:
        raise ValueError("product_code is mandatory.")

    if not cust_code:
        raise ValueError("cust_code is mandatory.")

    if not container_id:
        raise ValueError("container_id is mandatory.")