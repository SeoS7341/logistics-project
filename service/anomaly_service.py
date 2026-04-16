import pandas as pd
from datetime import datetime
from db.repository import AnomalyRepository
from service.slack_service import SlackService

class AnomalyService:

    def __init__(self):
        self.repo = AnomalyRepository()
        self.slack = SlackService()

    def save(self, anomalies):
        if not anomalies:
            print("✨ 이상 내역이 없습니다.")
            return

        # 1. DB 저장 (사후 분석용)
        self.repo.save_all(anomalies)

        # 2. 엑셀 리포트 생성 (현장 전달/보고용)
        self._generate_excel_report(anomalies)

        # 3. 슬랙 알림 (실시간 확인용)
        self.slack.notify(anomalies)

    def _generate_excel_report(self, anomalies):
            """내부 메서드: 이상 내역을 엑셀로 변환"""
            data = [{
                "업체명": a.biz_name,
                "상품명": a.product_name,
                "구분": a.issue_type,
                "주문량": a.order_qty,      # 수정됨
                "출고량": a.shipment_qty,   # 수정됨
                "차이": a.order_qty - a.shipment_qty  # 수정됨
            } for a in anomalies]
            
            df = pd.DataFrame(data)
            # 파일명에 날짜와 시간을 넣어 중복 방지
            filename = f"미매칭리포트_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(filename, index=False)
            
            print(f"\n📊 이상 내역 리포트 생성 완료: {filename}")
            print(f"🔎 총 {len(anomalies)}건의 [{anomalies[0].issue_type if anomalies else ''}] 관련 내역이 발견되었습니다.")