# db/repository_master.py

# =========================================
# container 존재 여부
# =========================================
def exists_container(
    db,
    container_id
):

    query = """
        SELECT COUNT(*)
        FROM containers
        WHERE container_id = :container_id
    """

    result = db.execute(query, {
        "container_id": container_id
    }).scalar()

    return result > 0


# =========================================
# 상품 존재 여부
# =========================================
def exists_product(
    db,
    product_code
):

    query = """
        SELECT COUNT(*)
        FROM mst_products
        WHERE product_code = :product_code
    """

    result = db.execute(query, {
        "product_code": product_code
    }).scalar()

    return result > 0


# =========================================
# 거래처 존재 여부
# =========================================
def exists_customer(
    db,
    cust_code
):

    query = """
        SELECT COUNT(*)
        FROM mst_customers
        WHERE cust_code = :cust_code
    """

    result = db.execute(query, {
        "cust_code": cust_code
    }).scalar()

    return result > 0