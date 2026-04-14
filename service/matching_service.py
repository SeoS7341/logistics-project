from domain.anomaly import Anomaly
from collections import defaultdict


class MatchingService:

    def find_mismatch(self, orders, shipments):
        order_map = defaultdict(int)
        shipment_map = defaultdict(int)

        # 주문 집계
        for o in orders:
            key = (o.biz_name, o.product_name)
            order_map[key] += o.quantity

        # 출고 집계
        for s in shipments:
            key = (s.biz_name, s.product_name)
            shipment_map[key] += s.quantity

        anomalies = []

        all_keys = set(order_map.keys()) | set(shipment_map.keys())

        for key in all_keys:
            o_qty = order_map.get(key, 0)
            s_qty = shipment_map.get(key, 0)

            if o_qty == 0:
                issue = "OVER"
            elif s_qty == 0:
                issue = "MISSING"
            elif o_qty != s_qty:
                issue = "MISMATCH"
            else:
                continue

            anomalies.append(Anomaly(
                biz_name=key[0],
                product_name=key[1],
                issue_type=issue,
                order_qty=o_qty,
                shipment_qty=s_qty
            ))

        return anomalies