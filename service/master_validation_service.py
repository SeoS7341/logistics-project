# service/master_validation_service.py

from db.repository_master import (
    exists_container,
    exists_product,
    exists_customer
)


def validate_master_data(
    db,
    payload: dict
):

    product_code = payload.get("product_code")
    cust_code = payload.get("cust_code")
    container_id = payload.get("container_id")

    # =========================================
    # 필수값 검증
    # =========================================
    if not product_code:
        raise ValueError("product_code is mandatory.")

    if not cust_code:
        raise ValueError("cust_code is mandatory.")

    if not container_id:
        raise ValueError("container_id is mandatory.")

    # =========================================
    # 상품 존재 여부 검증
    # =========================================
    if not exists_product(
        db=db,
        product_code=product_code
    ):

        raise ValueError(
            f"product not found: {product_code}"
        )

    # =========================================
    # 거래처 존재 여부 검증
    # =========================================
    if not exists_customer(
        db=db,
        cust_code=cust_code
    ):

        raise ValueError(
            f"customer not found: {cust_code}"
        )

    # =========================================
    # 컨테이너 존재 여부 검증
    # =========================================
    if not exists_container(
        db=db,
        container_id=container_id
    ):

        raise ValueError(
            f"container not found: {container_id}"
        )