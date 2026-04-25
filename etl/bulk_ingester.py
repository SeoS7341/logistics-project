import pandas as pd
import requests
import json
import os
import shutil
import datetime
import numpy as np

BASE_DIR = r"C:\SeoS\Claude\WorkResource\logistics-project"
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
API_URL = "http://127.0.0.1:8000/ingest/raw-metadata"

def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, pd.Timestamp)):
        return obj.isoformat()
    return str(obj) # 그 외 모든 객체는 문자열로 강제 변환

def run_ingestion():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(('.xlsx', '.xls', '.csv'))]
    
    if not files:
        print("[!] 처리할 파일이 없습니다.")
        return

    for filename in files:
        file_path = os.path.join(RAW_DIR, filename)
        
        if any(keyword in filename for keyword in ["씨제", "CJ", "TC", "장지"]):
            system_name = "CJ_FRESHWAY"
        elif "푸드팡" in filename:
            system_name = "FOODPANG"
        else:
            system_name = "LOGISTICS_ETC"
        
        print(f"[*] 처리 중: {filename} ({system_name})")
        
        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                engine = 'xlrd' if filename.endswith('.xls') else 'openpyxl'
                df = pd.read_excel(file_path, engine=engine)
            
            # [강력한 클리닝 1] 모든 숫자형 결측치(NaN, Inf)를 None으로 교체
            df = df.replace([np.inf, -np.inf], np.nan)
            
            success_count = 0
            for _, row in df.iterrows():
                # [강력한 클리닝 2] 각 row를 dict로 바꾼 후, JSON 호환 불가능한 값들 필터링
                raw_dict = row.to_dict()
                cleaned_dict = {}
                
                for k, v in raw_dict.items():
                    # null, nan, inf 체크 후 안전한 값만 할당
                    if pd.isna(v):
                        cleaned_dict[k] = None
                    else:
                        cleaned_dict[k] = v

                # JSON 직렬화 테스트 및 강제 변환
                json_payload_str = json.dumps(cleaned_dict, default=json_serial)
                final_extra_info = json.loads(json_payload_str)
                
                payload = {
                    "system_name": system_name,
                    "mall_id": str(final_extra_info.get("공급업체", "UNKNOWN_MALL")),
                    "data_type": "raw_upload",
                    "label_no": str(final_extra_info.get("릴리즈코드") or final_extra_info.get("라벨번호") or "N/A"),
                    "order_no": str(final_extra_info.get("OMS주문번호") or final_extra_info.get("주문번호") or "N/A"),
                    "extra_info": final_extra_info
                }

                response = requests.post(API_URL, json=payload)
                if response.status_code == 200:
                    success_count += 1
            
            print(f"  [V] {success_count}건 전송 완료.")
            
            today_folder = datetime.datetime.now().strftime("%Y-%m-%d")
            save_path = os.path.join(PROCESSED_DIR, today_folder)
            os.makedirs(save_path, exist_ok=True)
            shutil.move(file_path, os.path.join(save_path, filename))

        except Exception as e:
            print(f"  [X] 에러 발생: {e}")

if __name__ == "__main__":
    run_ingestion()