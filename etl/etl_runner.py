import os
import pandas as pd
from etl.loader.foodpang import FoodpangLoader
from etl.loader.neulpum import NeulpumLoader
from etl.loader.wellstory import WellstoryLoader
from etl.mapper import order_mapper

def distribute_tc_labels(tc_file_path: str, output_dir: str):
    """
    [Feature: distribute_tc_labels] 
    TC통합 파일을 읽어 업체/공정별로 분리하고 '순서' 및 '회차' 기반으로 정렬하여 .xls 추출
    """
    if not tc_file_path or not os.path.exists(tc_file_path):
        print(f"⚠️ TC통합 파일을 찾을 수 없습니다. 경로를 확인하세요: {tc_file_path}")
        return

    try:
        # 원본 데이터 로드
        df_total = pd.read_excel(tc_file_path)
    except Exception as e:
        print(f"❌ 파일 로드 중 에러 발생: {e}")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. 2차 업체 추출 및 '순서' 정렬
    minwoo_group = ['민우농산 - TC', '코테라 - TC', '인푸스 - TC']
    df_minwoo = df_total[df_total['공급업체'].isin(minwoo_group)].copy()
    df_minwoo = df_minwoo.sort_values(by='순서', ascending=True)
    df_minwoo.to_excel(os.path.join(output_dir, "processed_2차_민우코인.xls"), index=False)
    
    # 이삭 그룹 필터링
    df_isaac_total = df_total[df_total['공급업체'] == '이삭 - TC'].copy()
    
    # 2. 3차 이삭 1,2 (박스 제외) -> [순서 -> 회차] 다중 정렬
    df_isaac_12 = df_isaac_total[~df_isaac_total['품명'].str.contains('박스', na=False)].copy()
    df_isaac_12 = df_isaac_12.sort_values(by=['순서', '회차'], ascending=[True, True])
    df_isaac_12.to_excel(os.path.join(output_dir, "processed_3차_이삭12.xls"), index=False)
    
    # 3. 3차 이삭 박스 -> '순서' 정렬
    df_isaac_box = df_isaac_total[df_isaac_total['품명'].str.contains('박스', na=False)].copy()
    df_isaac_box = df_isaac_box.sort_values(by='순서', ascending=True)
    df_isaac_box.to_excel(os.path.join(output_dir, "processed_3차_이삭박스.xls"), index=False)

    print(f"✨ 엑셀 추출 완료: {output_dir} 폴더를 확인하세요.")

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
            if df is not None and not df.empty:
                all_dfs.append(df)
        else:
            print(f"⚠️ {loader.__class__.__name__}: 파일을 찾을 수 없어 스킵합니다.")

    # 3. 데이터 통합
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
    else:
        combined_df = pd.DataFrame(columns=["biz_name", "product_name", "quantity"])
    
    combined_df["date"] = config.get("target_date")

    # 4. Mapper를 통한 객체화
    orders = order_mapper.to_orders(combined_df)
    shipments = [] 

    # 5. [추가된 핵심 기능] TC 라벨 분배 실행
    tc_file = config.get("tc_file_name")
    if tc_file:
        # data/raw 폴더에 있는 파일을 찾아 data/processed 폴더로 출력
        distribute_tc_labels(os.path.join("data", "raw", tc_file), os.path.join("data", "processed"))

    return orders, shipments