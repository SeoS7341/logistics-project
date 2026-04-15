import pandas as pd
from .base_loader import BaseLoader

class WellstoryLoader(BaseLoader):
    def load(self, file_path: str) -> pd.DataFrame:
        df = pd.read_excel(file_path, engine="openpyxl")
        df.columns = df.columns.str.strip()
        return df.rename(columns={
            "업장명": "biz_name",
            "품명": "product_name",
            "수량": "quantity"
        })