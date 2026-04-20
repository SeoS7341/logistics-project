from .base_loader import BaseLoader


class NeulpumLoader(BaseLoader):

    REQUIRED_COLUMNS = {
        "주문사업장명": "biz_name",
        "품목명": "product_name",
        "라벨수량": "quantity"
    }