class Anomaly:
    def __init__(self, biz_name, product_name, issue_type, order_qty, shipment_qty):
        self.biz_name = biz_name
        self.product_name = product_name
        self.issue_type = issue_type  # MISSING / OVER / MISMATCH
        self.order_qty = order_qty
        self.shipment_qty = shipment_qty