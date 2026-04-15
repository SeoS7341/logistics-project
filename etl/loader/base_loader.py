from abc import ABC, abstractmethod
import pandas as pd

class BaseLoader(ABC):
    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame:
        """파일을 읽어 표준 컬럼(biz_name, product_name, quantity)으로 반환해야 함"""
        pass