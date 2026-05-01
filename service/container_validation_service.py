# service/container_validation_service.py

def validate_required_fields(payload: dict):

    label_no = payload.get("label_no")
    order_no = payload.get("order_no")

    if not label_no:
        raise ValueError("label_no is mandatory.")

    if not order_no:
        raise ValueError("order_no is mandatory.")