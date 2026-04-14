from domain.order import Order

def to_orders(df):
    orders = []

    for _, row in df.iterrows():
        orders.append(Order(
            biz_name=row["biz_name"],
            product_name=row["product_name"],
            quantity=row["quantity"],
            date=row.get("date")
        ))

    return orders