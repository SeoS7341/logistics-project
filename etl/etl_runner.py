import pandas as pd
from etl.loader.foodpang import FoodpangLoader
from etl.loader.neulpum import NeulpumLoader
from etl.loader.wellstory import WellstoryLoader
from etl.mapper import order_mapper

def run_all(config: dict):
    # 1. 사용할 로더들을 등록 (새 플랫폼 추가 시 여기만 한 줄 추가)
    registry = [
        (FoodpangLoader(), config.get("foodpang_path")),
        (NeulpumLoader(), config.get("neulpum_path")),
        (WellstoryLoader(), config.get("wellstory_path")),
    ]

    all_dfs = []

    # 2. 루프를 돌며 데이터 로드 및 표준화
    for loader, path in registry:
        if path:
            print(f"🚚 {loader.__class__.__name__} 로딩 중...")
            df = loader.load(path)
            all_dfs.append(df)

    if not all_dfs:
        print("⚠️ 로드할 데이터가 없습니다.")
        return [], []

    # 3. 데이터 통합
    orders_df = pd.concat(all_dfs, ignore_index=True)
    
    # 4. 날짜 정보 추가
    orders_df["date"] = config.get("target_date")

    # 5. 도메인 객체(Order)로 변환
    orders = order_mapper.to_orders(orders_df)
    
    # TODO: Playwright 기반 Shipment 수집 로직이 들어갈 자리
    shipments = [] 

    return orders, shipments