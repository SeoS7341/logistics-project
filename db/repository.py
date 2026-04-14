from db.connection import SessionLocal
from db.models import Order as OrderModel, Anomaly as AnomalyModel


class OrderRepository:

    def save_all(self, orders):
        session = SessionLocal()
        try:
            for o in orders:
                session.add(OrderModel(
                    biz_name=o.biz_name,
                    product_name=o.product_name,
                    quantity=o.quantity,
                    order_date=o.date
                ))
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class AnomalyRepository:

    def save_all(self, anomalies):
        session = SessionLocal()
        try:
            for a in anomalies:
                session.add(AnomalyModel(
                    biz_name=a.biz_name,
                    product_name=a.product_name,
                    issue_type=a.issue_type,
                    order_qty=a.order_qty,
                    shipment_qty=a.shipment_qty
                ))
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()