# etl/etl_runner.py
import os
import pandas as pd
from etl.loader.foodpang import FoodpangLoader
from etl.loader.neulpum import NeulpumLoader
from etl.loader.wellstory import WellstoryLoader
from etl.mapper import order_mapper

def run_all(config: dict):
    # 1. 사용할 로더들 등록
    registry = [
        (FoodpangLoader(), config.get("foodpang_path")),
        (NeulpumLoader(), config.get("neulpum_path")),
        (WellstoryLoader(), config.get("wellstory_path")),
    ]

    all_dfs = []

    # 2. 루프를 돌며 데이터 로드
    for loader, path in registry:
        if path and os.path.exists(path):
            print(f"🚚 {loader.__class__.__name__} 로딩 중... (Path: {path})")
            df = loader.load(path)
            # 데이터가 정상적으로 로드되었다면 리스트에 추가
            if df is not None and not df.empty:
                all_dfs.append(df)
        else:
            print(f"⚠️ {loader.__class__.__name__}: 파일을 찾을 수 없어 스킵합니다.")

    # 3. [핵심] 리스트에 담긴 데이터프레임들을 하나로 합치기
    if all_dfs:
        # 리스트를 하나의 DataFrame으로 변환
        combined_df = pd.concat(all_dfs, ignore_index=True)
    else:
        # 데이터가 없으면 규격에 맞는 빈 DataFrame 생성
        combined_df = pd.DataFrame(columns=["biz_name", "product_name", "quantity"])
    
    # 4. 날짜 정보 추가
    combined_df["date"] = config.get("target_date")

    # 5. 이제 combined_df(DataFrame)를 넘기므로 .iterrows() 에러가 나지 않습니다.
    orders = order_mapper.to_orders(combined_df)
    
    shipments = [] # Playwright 실적 수집은 다음 단계

    return orders, shipments