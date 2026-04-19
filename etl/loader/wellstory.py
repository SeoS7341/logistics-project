import pandas as pd
from .base_loader import BaseLoader

class WellstoryLoader(BaseLoader):
    def load(self, file_path: str) -> pd.DataFrame:
        try:
            # 1. 엔진 선택 및 로드
            engine = "xlrd" if file_path.endswith(".xls") else "openpyxl"
            df = pd.read_excel(file_path, engine=engine)
            
            # [디버깅용] 실제 엑셀에 어떤 컬럼이 있는지 출력해봅니다.
            print(f"🔍 엑셀 컬럼 확인: {df.columns.tolist()}")

            # 2. 공백 제거 및 이름 변경
            df.columns = df.columns.str.strip()
            
            # 실제 엑셀 컬럼명에 맞춰 매핑 (파일을 보니 '거래처명'일 확률이 높습니다)
            rename_map = {
                "업장명": "biz_name",
                "거래처명": "biz_name", # 추가: 웰스토리 파일은 '거래처명'으로 되어 있는 경우가 많음
                "품명": "product_name",
                "수량": "quantity",
                "주문": "quantity"      # 추가: '주문' 컬럼이 수량인 경우 대비
            }
            
            df = df.rename(columns=rename_map)
            
            # 필요한 컬럼이 없으면 빈 값이라도 채워줌 (KeyError 방지)
            for col in ["biz_name", "product_name", "quantity"]:
                if col not in df.columns:
                    df[col] = "" 
            
            return df
            
        except Exception as e:
            print(f"❌ WellstoryLoader 에러 ({file_path}): {e}")
            return pd.DataFrame()