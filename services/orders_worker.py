import json
from typing import Any, Dict, List


def process(event: Dict[str, Any], _context: Any) -> None:
    """
    SQS-triggered Lambda for order creation events.
    Expects messages delivered by EventBridge Rule -> SQS, shaped like:
      {"version":"0", "id":"...", "detail-type":"...", "source":"...", "detail": {...}}
    """
    records: List[Dict[str, Any]] = event.get("Records", [])
    for r in records:
        body = r.get("body", "{}")
        msg = json.loads(body)
        detail = msg.get("detail", {})
        event_name = detail.get("EventName")
        pk = detail.get("pk")
        sk = detail.get("sk")
        payload = detail.get("Payload")
        print(f"[OrdersWorker] EventName={event_name} pk={pk} sk={sk} payload={payload}")
        # TODO: implement create-order business logic
