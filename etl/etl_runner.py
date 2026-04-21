import warnings

warnings.filterwarnings(
    "ignore",
    message=".*xlwt.*",
    category=FutureWarning
)

import os
import pandas as pd

from etl.loader.foodpang import FoodpangLoader
from etl.loader.neulpum import NeulpumLoader
from etl.loader.wellstory import WellstoryLoader
from etl.mapper import order_mapper
from service.statistics_service import StatisticsService
from service.slack_service import SlackService
from service.anomaly_service import AnomalyService
from config import Config

# ==============================
# 공통 저장 함수
# ==============================
def save_legacy_excel(df, path):
    """iLabel2 호환용 (.xls)"""
    try:
        df.to_excel(path, index=False, engine="xlwt")
    except Exception as e:
        print(f"❌ XLS 저장 실패: {e}")


def save_modern_excel(df, path):
    """내부 분석/백업용 (.xlsx)"""
    try:
        df.to_excel(path, index=False)
    except Exception as e:
        print(f"❌ XLSX 저장 실패: {e}")


# ==============================
# 1. Extract
# ==============================
def extract(registry):
    dfs = []

    for loader, path in registry:
        if path and os.path.exists(path):
            print(f"🚚 {loader.__class__.__name__} 로딩 중... (Path: {path})")
            df = loader.load(path)

            if df is not None and not df.empty:
                dfs.append(df)
        else:
            print(f"⚠️ {loader.__class__.__name__}: 파일 없음, 스킵")

    return dfs


# ==============================
# 2. Transform (Staging Layer)
# ==============================
def transform(dfs, target_date):
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["biz_name", "product_name", "quantity"])

    df["date"] = target_date

    return df


# ==============================
# 3. Load (현재는 placeholder)
# ==============================
def load(df):
    print(f"💾 Load 완료: {len(df)} rows")
    # TODO: DB 저장 또는 파일 저장


# ==============================
# 4. Aggregate (Mart Layer)
# ==============================
def aggregate(df):
    df["order_count"] = 1

    result = df.groupby("biz_name").agg({
        "quantity": "sum",
        "order_count": "sum"
    }).reset_index()

    return result


# ==============================
# 5. Post Process (비즈니스 로직)
# ==============================
def distribute_tc_labels(tc_file_path: str, output_dir: str):
    if not tc_file_path or not os.path.exists(tc_file_path):
        print(f"⚠️ TC통합 파일 없음: {tc_file_path}")
        return

    try:
        df_total = pd.read_excel(tc_file_path)
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 2차 그룹
    minwoo_group = ['민우농산 - TC', '코테라 - TC', '인푸스 - TC']
    df_minwoo = df_total[df_total['공급업체'].isin(minwoo_group)].copy()
    df_minwoo = df_minwoo.sort_values(by='순서')

    save_legacy_excel(df_minwoo, os.path.join(output_dir, "processed_2차_민우코인.xls"))
    save_modern_excel(df_minwoo, os.path.join(output_dir, "processed_2차_민우코인.xlsx"))

    # 이삭
    df_isaac = df_total[df_total['공급업체'] == '이삭 - TC'].copy()

    df_isaac_12 = df_isaac[~df_isaac['품명'].str.contains('박스', na=False)].copy()
    df_isaac_12 = df_isaac_12.sort_values(by=['순서', '회차'])

    save_legacy_excel(df_isaac_12, os.path.join(output_dir, "processed_3차_이삭12.xls"))
    save_modern_excel(df_isaac_12, os.path.join(output_dir, "processed_3차_이삭12.xlsx"))

    df_isaac_box = df_isaac[df_isaac['품명'].str.contains('박스', na=False)].copy()
    df_isaac_box = df_isaac_box.sort_values(by='순서')

    save_legacy_excel(df_isaac_box, os.path.join(output_dir, "processed_3차_이삭박스.xls"))
    save_modern_excel(df_isaac_box, os.path.join(output_dir, "processed_3차_이삭박스.xlsx"))

    print(f"✨ TC 라벨 분배 완료 → {output_dir}")


def run_post_process(config):
    tc_file = config.get("tc_file_name")

    if tc_file:
        distribute_tc_labels(
            os.path.join("data", "raw", tc_file),
            os.path.join("data", "processed")
        )


# ==============================
# Main Pipeline
# ==============================
def run_all(config: dict):
    registry = [
        (FoodpangLoader(), config.get("foodpang_path")),
        (NeulpumLoader(), config.get("neulpum_path")),
        (WellstoryLoader(), config.get("wellstory_path")),
    ]

    # 1. Extract
    raw_dfs = extract(registry)

    # 2. Transform
    staging_df = transform(raw_dfs, config.get("target_date"))

    # 3. Load
    load(staging_df)

    # 4. Domain Mapping
    orders = order_mapper.to_orders(staging_df)

    # 5. Aggregate
    mart = aggregate(staging_df)

    # 6. KPI
    kpi = StatisticsService.build_kpi(staging_df)
    save_kpi(kpi)

    # 7. Post Process (라벨)
    run_post_process(config)

    # 8. 🔥 Anomaly Detection (핵심)
    anomaly_service = AnomalyService()
    # 👉 지금은 shipment 없으니까 None
    anomalies = anomaly_service.detect(staging_df, None)

    # 9. 🔥 저장 + Slack 알림
    anomaly_service.process(anomalies)

    return orders, mart, kpi
    
def save_kpi(kpi: dict, output_dir="data/processed/kpi"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for name, df in kpi.items():
        path = os.path.join(output_dir, f"{name}.xlsx")
        df.to_excel(path, index=False)

    print(f"📊 KPI 파일 저장 완료 → {output_dir}")