# 📦 Logistics Data Pipeline

## 📌 프로젝트 개요
물류 회사에서 수작업으로 처리되던 Excel 데이터를 자동으로 수집/정제/분석하는 ETL 파이프라인을 구축했습니다.

---

## ⚙️ 아키텍처

Extract → Transform → Load → Mart → Analytics → PostProcess

---

## 🚀 주요 기능

### 1. 다중 Excel Loader
- Foodpang / Neulpum / Wellstory 데이터 통합 처리
- Template Method 기반 Loader 설계

### 2. 데이터 표준화
- biz_name / product_name / quantity 스키마 통일
- 컬럼 검증 및 타입 정제

### 3. KPI 분석
- 거래처별 주문량
- 상품별 주문량
- Top N 분석
- SKU 다양성 분석

### 4. 라벨 출력 자동화
- iLabel2 시스템 대응 (.xls)
- 내부 분석용 (.xlsx) 병행 저장

---

## 🧠 설계 포인트

- 레거시 시스템(.xls)과 최신 포맷(.xlsx) 병행 처리
- Loader 공통화 (Template Method 패턴)
- Analytics Layer 분리

---

## 🛠 기술 스택

- Python
- Pandas
- Batch Processing

---

## 📊 실행 결과

- 하루 약 8,000건 주문 데이터 처리
- KPI 자동 생성 및 파일 저장
- 라벨 출력 자동화

---

## 💡 문제 해결

기존:
- 수작업 Excel 처리
- 반복 작업
- 오류 발생

개선:
- 자동화 ETL 파이프라인 구축
- 데이터 품질 보장
- 분석 지표 자동 생성