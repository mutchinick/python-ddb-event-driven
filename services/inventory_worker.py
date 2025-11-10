import json
from typing import Any, Dict, List


def process(event: Dict[str, Any], _context: Any) -> None:
    """
    SQS-triggered Lambda for stock adjustment events.
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
        print(f"[InventoryWorker] EventName={event_name} pk={pk} sk={sk} payload={payload}")
        # TODO: implement inventory adjustment logic
