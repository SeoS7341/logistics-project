from sqlalchemy import Column, Integer, String
from db.connection import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    biz_name = Column(String(100))
    product_name = Column(String(200))
    quantity = Column(Integer)
    order_date = Column(String(20))


class Anomaly(Base):
    __tablename__ = "anomalies"

    id = Column(Integer, primary_key=True, index=True)
    biz_name = Column(String(100))
    product_name = Column(String(200))
    issue_type = Column(String(20))
    order_qty = Column(Integer)
    shipment_qty = Column(Integer)