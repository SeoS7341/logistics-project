class SlackService:

    def notify(self, anomalies):
        print("🚨 이상 데이터 발생")

        for a in anomalies:
            print(f"[{a.issue_type}] {a.biz_name} / {a.product_name} "
                  f"주문:{a.order_qty} 출고:{a.shipment_qty}")