import pandas as pd
from .base_loader import BaseLoader

class FoodpangLoader(BaseLoader):
    def load(self, file_path: str) -> pd.DataFrame:
        df = pd.read_excel(file_path, engine="openpyxl")
        df.columns = df.columns.str.strip()
        return df.rename(columns={
            "주문사업장명": "biz_name",
            "품목명": "product_name",
            "라벨수량": "quantity"
        })