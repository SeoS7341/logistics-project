from db.repository import AnomalyRepository
from service.slack_service import SlackService


class AnomalyService:

    def __init__(self):
        self.repo = AnomalyRepository()
        self.slack = SlackService()

    def save(self, anomalies):
        if not anomalies:
            return

        self.repo.save_all(anomalies)
        self.slack.notify(anomalies)