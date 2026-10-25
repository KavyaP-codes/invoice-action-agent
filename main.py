
import os
from fastapi import FastAPI

app = FastAPI(
    title="Invoice Action Agent",
    version="1.0.0",
)

BASE_URL = os.getenv(
    "PUBLIC_BASE_URL",
    "https://invoice-action-agent-1fbn.onrender.com/a2a/",
)

@app.get("/")
def home():
    return {
        "service": "Invoice Action Agent",
        "status": "running",
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/.well-known/agent-card.json")
def agent_card():
    return {
        "name": "Invoice Action Agent",
        "description": (
            "Evidence-based invoice decisions, "
            "approval-gated actions and safe receipts."
        ),
        "version": "1.0.0",
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
        },
        "supportedInterfaces": [
            {
                "url": BASE_URL,
                "protocolBinding": "HTTP+JSON",
                "protocolVersion": "1.0",
            }
        ],
        "defaultInputModes": [
            "application/vnd.ga5.invoice-claim-batch+json"
        ],
        "defaultOutputModes": [
            "application/vnd.ga5.invoice-action-proposals+json",
            "application/vnd.ga5.invoice-action-receipts+json",
        ],
        "skills": [
            {
                "id": "invoice_action_agent",
                "name": "Invoice Action Agent",
                "description": (
                    "Review invoice evidence and propose "
                    "actions with verifiable receipts."
                ),
                "tags": ["invoice", "evidence", "approvals"],
            }
        ],
    }
