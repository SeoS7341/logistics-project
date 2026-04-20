import pandas as pd


class StatisticsService:

    @staticmethod
    def summary_by_biz(df: pd.DataFrame) -> pd.DataFrame:
        """거래처별 주문량"""
        return df.groupby("biz_name").agg({
            "quantity": "sum"
        }).reset_index().sort_values(by="quantity", ascending=False)

    @staticmethod
    def summary_by_product(df: pd.DataFrame) -> pd.DataFrame:
        """상품별 주문량"""
        return df.groupby("product_name").agg({
            "quantity": "sum"
        }).reset_index().sort_values(by="quantity", ascending=False)

    @staticmethod
    def sku_count_by_biz(df: pd.DataFrame) -> pd.DataFrame:
        """거래처별 상품 다양성"""
        return df.groupby("biz_name")["product_name"].nunique().reset_index(name="sku_count")

    @staticmethod
    def top_n_biz(df: pd.DataFrame, n=5) -> pd.DataFrame:
        return (
            df.groupby("biz_name")["quantity"]
            .sum()
            .reset_index()
            .sort_values(by="quantity", ascending=False)
            .head(n)
        )

    @staticmethod
    def top_n_product(df: pd.DataFrame, n=5) -> pd.DataFrame:
        return (
            df.groupby("product_name")["quantity"]
            .sum()
            .reset_index()
            .sort_values(by="quantity", ascending=False)
            .head(n)
        )

    @staticmethod
    def build_kpi(df: pd.DataFrame) -> dict:
        """전체 KPI 묶음"""
        return {
            "by_biz": StatisticsService.summary_by_biz(df),
            "by_product": StatisticsService.summary_by_product(df),
            "sku_by_biz": StatisticsService.sku_count_by_biz(df),
            "top5_biz": StatisticsService.top_n_biz(df, 5),
            "top5_product": StatisticsService.top_n_product(df, 5),
        }