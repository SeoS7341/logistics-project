import pandas as pd

def load(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, engine="openpyxl")

    # 컬럼 정리 (필수)
    df.columns = df.columns.str.strip()

    print("neulpum 컬럼:", df.columns.tolist())

    return df.rename(columns={
        "주문사업장명": "biz_name",
        "품목명": "product_name",
        "라벨수량": "quantity"
    })