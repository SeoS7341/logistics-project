import pandas as pd
from datetime import datetime
from domain.anomaly import Anomaly
from db.repository import AnomalyRepository
from service.slack_service import SlackService
from config import Config


class AnomalyService:

    def __init__(self):
        self.repo = AnomalyRepository()

    # ==============================
    # 1. 탐지 (핵심 로직)
    # ==============================
    @staticmethod
    def detect(order_df, shipment_df=None):
        anomalies = []

        # 주문 집계
        order_map = order_df.groupby(
            ["biz_name", "product_name"]
        )["quantity"].sum().to_dict()

        # 출고 집계 (없으면 0 처리)
        if shipment_df is not None:
            shipment_map = shipment_df.groupby(
                ["biz_name", "product_name"]
            )["quantity"].sum().to_dict()
        else:
            shipment_map = {}

        # 전체 키
        keys = set(order_map.keys()) | set(shipment_map.keys())

        for key in keys:
            o_qty = order_map.get(key, 0)
            s_qty = shipment_map.get(key, 0)

            biz_name, product_name = key

            # 정상 데이터 스킵
            if o_qty == s_qty:
                continue

            # 유형 분류
            if o_qty == 0:
                issue = "OVER_SHIPMENT"
                desc = "주문 없이 출고"
            elif s_qty == 0:
                issue = "NO_SHIPMENT"
                desc = "출고 없음"
            else:
                issue = "MISMATCH"
                desc = "주문/출고 불일치"

            anomalies.append(Anomaly(
                issue_type=issue,
                biz_name=biz_name,
                product_name=product_name,
                order_qty=o_qty,
                shipment_qty=s_qty,
                description=desc
            ))

        return anomalies

    # ==============================
    # 2. 저장 + 리포트 + 알림
    # ==============================
    def process(self, anomalies):
        if not anomalies:
            print("✨ 이상 없음")
            return

        # 1. DB 저장
        self.repo.save_all(anomalies)

        # 2. 엑셀 리포트
        self._save_excel(anomalies)

        # 3. Slack 알림
        SlackService.notify(Config.SLACK_WEBHOOK_URL, anomalies)

    # ==============================
    # 내부 메서드
    # ==============================
    def _save_excel(self, anomalies):
        data = [{
            "업체명": a.biz_name,
            "상품명": a.product_name,
            "구분": a.issue_type,
            "주문량": a.order_qty,
            "출고량": a.shipment_qty,
            "차이": a.order_qty - a.shipment_qty
        } for a in anomalies]

        df = pd.DataFrame(data)

        output_dir = "data/processed"
        filename = f"{output_dir}/anomaly_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

        df.to_excel(filename, index=False)

        print(f"📊 이상 리포트 저장 완료 → {filename}")