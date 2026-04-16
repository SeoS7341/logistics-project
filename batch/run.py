from etl.etl_runner import run_all
# from etl.mapper.order_mapper import to_orders  <-- 이제 여기서 직접 부를 필요가 없습니다.
from service.matching_service import MatchingService
from service.anomaly_service import AnomalyService
from db.repository import OrderRepository
from db.connection import engine
from db.models import Base

CONFIG = {
    "foodpang_path": "푸드팡주문내역.xlsx",
    "neulpum_path": "늘품주문내역.xlsx",
    "target_date": "2026-04-16" # 날짜 추가
}

def init_db():
    Base.metadata.create_all(bind=engine)

def run():
    # 0. DB 초기화
    init_db()

    # 1. ETL 실행 (runner 내부에서 이미 to_orders가 실행되어 객체 리스트가 반환됨)
    orders, shipments = run_all(CONFIG)

    # [검증 출력 추가]
    print(f"\n✅ 통합 완료: 총 {len(orders)}건의 주문 객체가 생성되었습니다.")
    for o in orders[:5]: 
        print(f"-> [객체확인] 업체: {o.biz_name:15} | 상품: {o.product_name:30} | 수량: {o.quantity}")

    # 2. DB 저장 (이미 객체 리스트이므로 바로 저장 가능)
    if orders:
        OrderRepository().save_all(orders)

    # 3. 매칭 및 이상 처리
    matching = MatchingService()
    anomalies = matching.find_mismatch(orders, shipments)

    if anomalies:
        anomaly_service = AnomalyService()
        anomaly_service.save(anomalies)
        print(f"🚨 이상 데이터 {len(anomalies)}건 발견 및 처리 완료")
    else:
        print("✨ 모든 데이터가 정상적으로 매칭되었습니다.")

if __name__ == "__main__":
    run()