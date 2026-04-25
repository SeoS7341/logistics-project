import pandas as pd
import requests
import os
import shutil
import datetime
import numpy as np
import re

# -----------------------------
# 경로 설정
# -----------------------------
BASE_DIR = r"C:\SeoS\Claude\WorkResource\logistics-project"
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
API_URL = "http://127.0.0.1:8000/ingest/raw-metadata"


# -----------------------------
# 문자열 정제
# -----------------------------
def clean_text(text):
    if not isinstance(text, str):
        return text

    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def normalize_keys(d):
    return {clean_text(k): v for k, v in d.items()}


# -----------------------------
# CSV 인코딩 대응
# -----------------------------
def read_csv_with_encoding(file_path):
    encodings = ['cp949', 'utf-8-sig', 'utf-8']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc, index_col=False)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(f"파일을 읽을 수 없습니다: {file_path}")


# -----------------------------
# 🔥 normalized 추출
# -----------------------------
def extract_normalized_fields(data):
    return {
        "product_code": data.get("사방넷 상품코드"),
        "product_name": data.get("사방넷 상품명"),
        "vendor_product_name": data.get("거래처 상품명"),
        "spec": data.get("거래처 규격"),
        "barcode": data.get("상품바코드"),
        "order_no": data.get("OMS주문번호") or data.get("주문번호"),
    }


# -----------------------------
# 메인 실행
# -----------------------------
def run_ingestion():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    files = [f for f in os.listdir(RAW_DIR) if f.endswith(('.xlsx', '.xls', '.csv'))]

    if not files:
        print("[!] 처리할 파일이 없습니다.")
        return

    for filename in files:
        file_path = os.path.join(RAW_DIR, filename)

        # 시스템 분류
        if any(keyword in filename for keyword in ["씨제", "CJ", "TC", "장지"]):
            system_name = "CJ_FRESHWAY"
        elif "푸드팡" in filename:
            system_name = "FOODPANG"
        else:
            system_name = "LOGISTICS_ETC"

        print(f"[*] 처리 중: {filename} ({system_name})")

        try:
            # 1. 파일 로드
            if filename.endswith('.csv'):
                df = read_csv_with_encoding(file_path)
            else:
                engine = 'xlrd' if filename.endswith('.xls') else 'openpyxl'
                df = pd.read_excel(file_path, engine=engine)

            # 2. 컬럼명 정제
            df.columns = [clean_text(col) for col in df.columns]

            # 🔥 3. 숫자 컬럼 제거 (0,1,2 같은 쓰레기 컬럼)
            df = df.loc[:, ~df.columns.astype(str).str.match(r'^\d+$')]

            # 4. 수치형 이상값 처리
            df = df.replace([np.inf, -np.inf], np.nan)

            # 5. 빈 행 제거
            df = df.dropna(how='all')

            # 6. 공백 문자열 제거
            df = df.applymap(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

            success_count = 0
            fail_count = 0

            for _, row in df.iterrows():
                raw_dict = row.to_dict()

                cleaned_dict = normalize_keys({
                    k: (None if pd.isna(v) else v)
                    for k, v in raw_dict.items()
                })

                # 🔥 완전 빈 데이터 방어
                if all(v is None for v in cleaned_dict.values()):
                    continue

                # 🔥 normalized 생성
                normalized = extract_normalized_fields(cleaned_dict)

                # 🔥 핵심: 필수값 없으면 "버리는게 아니라 NULL 처리"
                issue_type = None
                if not normalized.get("product_code"):
                    normalized = None
                    issue_type = "DATA_MISSING"

                payload = {
                    "system_name": system_name,
                    "mall_id": str(
                        cleaned_dict.get("공급업체")
                        or cleaned_dict.get("공급처명")
                        or "UNKNOWN_MALL"
                    ),
                    "data_type": "raw_upload",

                    # 🔥 핵심 구조
                    "normalized": normalized,

                    "label_no": str(
                        (normalized.get("barcode") if normalized else None)
                        or cleaned_dict.get("라벨번호")
                        or "N/A"
                    ),
                    "order_no": str(
                        (normalized.get("order_no") if normalized else None)
                        or cleaned_dict.get("사방넷 상품코드")
                        or "N/A"
                    ),

                    # 🔥 anomaly tagging
                    "issue_type": issue_type,

                    # 🔥 원본 보존
                    "extra_info": cleaned_dict
                }

                try:
                    response = requests.post(API_URL, json=payload, timeout=5)

                    if response.status_code == 200:
                        success_count += 1
                    else:
                        fail_count += 1
                        print(f"[!] 실패: {response.status_code} / {response.text}")

                except Exception as e:
                    fail_count += 1
                    print(f"[!] API 오류: {e}")

            print(f"  [V] 성공: {success_count}건 / 실패: {fail_count}건")

            # 7. 파일 이동
            today_folder = datetime.datetime.now().strftime("%Y-%m-%d")
            save_path = os.path.join(PROCESSED_DIR, today_folder)
            os.makedirs(save_path, exist_ok=True)

            shutil.move(file_path, os.path.join(save_path, filename))

        except Exception as e:
            print(f"  [X] 에러 발생 ({filename}): {e}")


if __name__ == "__main__":
    run_ingestion()