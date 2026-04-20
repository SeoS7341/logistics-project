import pandas as pd
from .base_loader import BaseLoader


class WellstoryLoader(BaseLoader):

    COLUMN_CANDIDATES = {
        "biz_name": ["업장명", "거래처명"],
        "product_name": ["품명"],
        "quantity": ["수량", "주문"]
    }

    def load(self, file_path: str) -> pd.DataFrame:
        try:
            engine = "xlrd" if file_path.endswith(".xls") else "openpyxl"
            df = pd.read_excel(file_path, engine=engine)

            print(f"🔍 엑셀 컬럼 확인: {df.columns.tolist()}")

            df.columns = df.columns.str.strip()

            # 1. 컬럼 매핑 자동 탐색
            rename_map = {}

            for target_col, candidates in self.COLUMN_CANDIDATES.items():
                for col in candidates:
                    if col in df.columns:
                        rename_map[col] = target_col
                        break

            df = df.rename(columns=rename_map)

            # 2. 필수 컬럼 검증
            missing = [col for col in ["biz_name", "product_name", "quantity"] if col not in df.columns]
            if missing:
                raise ValueError(f"❌ WellstoryLoader - 필수 컬럼 누락: {missing}")

            # 3. BaseLoader의 후처리 로직 재사용
            return self._post_process(df)

        except Exception as e:
            print(f"❌ WellstoryLoader 에러 ({file_path}): {e}")
            return pd.DataFrame()

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[["biz_name", "product_name", "quantity"]]

        df = df.dropna(subset=["biz_name", "product_name"])

        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

        df["biz_name"] = df["biz_name"].astype(str).str.strip()
        df["product_name"] = df["product_name"].astype(str).str.strip()

        print(f"✅ WellstoryLoader 로드 완료: {len(df)} rows")

        return df