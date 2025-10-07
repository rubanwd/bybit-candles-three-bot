import requests
from typing import Optional

class TelegramClient:
    def __init__(self, token: str, chat_id: str, signal_chat_id: Optional[str] = None):
        self.token = token
        self.chat_id = chat_id
        self.signal_chat_id = signal_chat_id or chat_id

    def send(self, text: str, to_signal: bool=False, disable_web_page_preview: bool = True) -> Optional[int]:
        chat = self.signal_chat_id if to_signal else self.chat_id
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        r = requests.post(url, json={
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview
        }, timeout=30)
        if r.ok:
            return r.json().get("result", {}).get("message_id")
        return None
