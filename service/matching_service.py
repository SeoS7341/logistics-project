from domain.anomaly import Anomaly


class MatchingService:

    @staticmethod
    def match(order_df, shipment_df):
        result = []

        order_map = order_df.groupby(
            ["biz_name", "product_name"]
        )["quantity"].sum().to_dict()

        shipment_map = shipment_df.groupby(
            ["biz_name", "product_name"]
        )["quantity"].sum().to_dict()

        keys = set(order_map.keys()) | set(shipment_map.keys())

        for key in keys:
            o_qty = order_map.get(key, 0)
            s_qty = shipment_map.get(key, 0)

            biz_name, product_name = key

            # 🔥 이상만 추출
            if o_qty == s_qty:
                continue

            if o_qty == 0:
                issue = "OVER_SHIPMENT"
            elif s_qty == 0:
                issue = "NO_SHIPMENT"
            else:
                issue = "MISMATCH"

            result.append(Anomaly(
                issue_type=issue,
                biz_name=biz_name,
                product_name=product_name,
                order_qty=o_qty,
                shipment_qty=s_qty
            ))

        return result