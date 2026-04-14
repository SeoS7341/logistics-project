import pandas as pd

def load(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, engine="openpyxl")

    return df.rename(columns={
        "업장명": "biz_name",
        "품명": "product_name",
        "수량": "quantity"
    })