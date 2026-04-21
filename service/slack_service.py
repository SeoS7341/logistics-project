import requests
import json


class SlackService:

    @staticmethod
    def send_message(webhook_url: str, message: str):
        if not webhook_url:
            print("⚠️ Slack Webhook URL 없음, 스킵")
            return

        payload = {
            "text": message
        }

        try:
            response = requests.post(
                webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=3
            )

            if response.status_code != 200:
                print(f"❌ Slack 전송 실패: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"❌ Slack 에러: {e}")

    @staticmethod
    def notify(webhook_url: str, anomalies):
        if not anomalies:
            print("ℹ️ 이상 없음")
            return

        print("🚨 이상 데이터 발생")

        MAX_LINES = 20
        lines = []

        for a in anomalies[:MAX_LINES]:
            line = f"[{a.issue_type}] {a.biz_name} / {a.product_name} 주문:{a.order_qty} 출고:{a.shipment_qty} 비고: {a.description}"
            print(line)
            lines.append(line)

        if len(anomalies) > MAX_LINES:
            lines.append(f"...외 {len(anomalies) - MAX_LINES}건 생략")

        message = "🚨 *이상 데이터 발생*\n\n" + "\n".join(lines)

        SlackService.send_message(webhook_url, message)