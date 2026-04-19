from etl.etl_runner import run_all

config = {
    "target_date": "2026-04-08",
    "tc_file_name": "260408_TC통합.xls", # data/raw에 이 파일이 있어야 합니다.
    "wellstory_path": "data/raw/260408_TC통합.xls" # 테스트용으로 동일 파일 지정 가능
}

run_all(config)