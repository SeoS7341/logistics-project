from etl.etl_runner import run_all
from etl.mapper.order_mapper import to_orders
from service.matching_service import MatchingService
from service.anomaly_service import AnomalyService
from db.repository import OrderRepository
from db.connection import engine
from db.models import Base

CONFIG = {
    "foodpang_path": "푸드팡주문내역.xlsx",
    "neulpum_path": "늘품주문내역.xlsx"
}


def init_db():
    Base.metadata.create_all(bind=engine)


def run():
    # 0. DB 초기화
    init_db()

    # 1. ETL
    orders_df, shipments_df = run_all(CONFIG)

    # 2. Domain 변환
    orders = to_orders(orders_df)
    shipments = to_orders(shipments_df)

    # 3. DB 저장
    OrderRepository().save_all(orders)

    # 4. 매칭
    matching = MatchingService()
    anomalies = matching.find_mismatch(orders, shipments)

    # 5. 이상 처리
    anomaly_service = AnomalyService()
    anomaly_service.save(anomalies)


if __name__ == "__main__":
    run()