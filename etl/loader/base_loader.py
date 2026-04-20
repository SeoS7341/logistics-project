import pandas as pd
from abc import ABC


class BaseLoader(ABC):

    REQUIRED_COLUMNS = {}

    def load(self, file_path: str) -> pd.DataFrame:
        df = pd.read_excel(file_path, engine="openpyxl")

        # 1. 컬럼 공백 제거
        df.columns = df.columns.str.strip()

        # 2. 컬럼 검증
        missing_cols = [col for col in self.REQUIRED_COLUMNS.keys() if col not in df.columns]
        if missing_cols:
            raise ValueError(f"❌ {self.__class__.__name__} - 필수 컬럼 누락: {missing_cols}")

        # 3. 컬럼 표준화
        df = df.rename(columns=self.REQUIRED_COLUMNS)

        # 4. 스키마 고정
        df = df[["biz_name", "product_name", "quantity"]]

        # 5. null 제거
        df = df.dropna(subset=["biz_name", "product_name"])

        # 6. 타입 정리
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

        # 7. 문자열 정리
        df["biz_name"] = df["biz_name"].astype(str).str.strip()
        df["product_name"] = df["product_name"].astype(str).str.strip()

        print(f"✅ {self.__class__.__name__} 로드 완료: {len(df)} rows")

        return df