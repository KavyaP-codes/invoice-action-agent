import os, json, hashlib, uuid, re, threading, logging
from datetime import datetime, timezone
from typing import Any, Optional
from fastapi import FastAPI, Request, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, String, Text, DateTime, UniqueConstraint, select
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("invoice_action_agent")

APP_VERSION = "1.0.0"
INPUT_MODE = "application/vnd.ga5.invoice-claim-batch+json"
PROPOSAL_MODE = "application/vnd.ga5.invoice-action-proposals+json"
RESULT_MODE = "application/vnd.ga5.invoice-action-results+json"
RECEIPT_MODE = "application/vnd.ga5.invoice-action-receipts+json"
A2A_MEDIA = "application/a2a+json"
ACTIONS = {"settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"}

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://invoice-action-agent-1fbn.onrender.com/a2a/").rstrip("/") + "/"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////tmp/invoice_agent.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=5, future=True)
Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
Base = declarative_base()

class StoredTask(Base):
    __tablename__ = "a2a_tasks"
    task_id = Column(String(80), primary_key=True)
    principal = Column(String(64), nullable=False, index=True)
    context_id = Column(String(80), nullable=False)
    state = Column(String(40), nullable=False)
    task_json = Column(Text, nullable=False)
    batch_id = Column(String(256), nullable=True)
    package_cache_keys = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

class Idempotency(Base):
    __tablename__ = "a2a_idempotency"
    principal = Column(String(64), primary_key=True)
    message_id = Column(String(256), primary_key=True)
    message_hash = Column(String(64), nullable=False)
    task_id = Column(String(80), nullable=False)

class DecisionCache(Base):
    __tablename__ = "invoice_decision_cache"
    cache_key = Column(String(64), primary_key=True)
    decision_json = Column(Text, nullable=False)

Base.metadata.create_all(engine)

app = FastAPI(title="Invoice Action Agent", version=APP_VERSION)
lock = threading.RLock()

def now(): return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
def canon(x): return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
def digest(x): return hashlib.sha256(canon(x).encode()).hexdigest()
def principal_hash(token): return hashlib.sha256(token.encode()).hexdigest()
def make_id(prefix): return prefix + "_" + uuid.uuid4().hex

def err(status, code, message):
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}}, media_type=A2A_MEDIA)

@app.middleware("http")
async def response_media(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/a2a/") and response.status_code < 400:
        response.headers["Content-Type"] = A2A_MEDIA
    return response

@app.get("/")
def home(): return {"service":"Invoice Action Agent", "status":"running"}
@app.get("/health")
def health(): return {"status":"ok"}

@app.get("/.well-known/agent-card.json")
def agent_card():
    return JSONResponse({
        "name":"Invoice Action Agent",
        "description":"Evidence-based invoice decisions, approval-gated actions and safe receipts.",
        "version":APP_VERSION,
        "capabilities":{"streaming":False,"pushNotifications":False},
        "supportedInterfaces":[{"url":PUBLIC_BASE_URL,"protocolBinding":"HTTP+JSON","protocolVersion":"1.0"}],
        "defaultInputModes":[INPUT_MODE],
        "defaultOutputModes":[PROPOSAL_MODE,RECEIPT_MODE],
        "skills":[{"id":"invoice_action_agent","name":"Invoice Action Agent","description":"Review invoice evidence and propose one safe action per invoice package, with verifiable evidence and receipts.","tags":["invoice","evidence","approvals","duplicate-payment-prevention"]}]
    })

def auth_and_protocol(request: Request, authorization: Optional[str], version: Optional[str], content_type: Optional[str], require_body=True):
    if not authorization or not authorization.startswith("Bearer ") or not authorization[7:].strip():
        raise HTTPException(401, detail={"code":"UNAUTHENTICATED","message":"Bearer authentication required"})
    if version != "1.0": raise HTTPException(400, detail={"code":"INVALID_A2A_VERSION","message":"A2A-Version must be 1.0"})
    if require_body and (not content_type or content_type.split(";")[0].strip().lower() != A2A_MEDIA):
        raise HTTPException(415, detail={"code":"UNSUPPORTED_MEDIA_TYPE","message":"Content-Type must be application/a2a+json"})
    return principal_hash(authorization[7:].strip())

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, dict) else {"code":"REQUEST_ERROR","message":"Request could not be completed"}
    return JSONResponse(status_code=exc.status_code, content={"error":detail}, media_type=A2A_MEDIA)

def get_message_part(message, media_type):
    for p in message.get("parts", []) or []:
        if p.get("mediaType") == media_type and isinstance(p.get("data"), dict): return p["data"]
    return None

def package_text(package):
    # Preserve all source document content; don't discard paragraph-level references.
    return canon(package)

def find_value(package, names, default=""):
    if isinstance(package, dict):
        for k,v in package.items():
            if k.lower().replace("_", "") in [n.lower().replace("_", "") for n in names] and not isinstance(v, (dict,list)) and v is not None:
                return v
        for v in package.values():
            got=find_value(v,names,None)
            if got is not None and got != "": return got
    elif isinstance(package, list):
        for v in package:
            got=find_value(v,names,None)
            if got is not None and got != "": return got
    return default

def extract_evidence_strings(obj):
    found=[]
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                if isinstance(v,str):
                    # Evidence references are typically bracketed identifiers such as [DOC-1 §2 ¶3].
                    if re.search(r"\[[^\]]{2,120}\]", v):
                        found.extend(re.findall(r"\[[^\]]{2,120}\]",v))
                else: walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
    walk(obj)
    return list(dict.fromkeys(found))

def model_decisions(packages):
    keys=[digest(p) for p in packages]
    out={}
    with Session() as db:
        for k in keys:
            row=db.get(DecisionCache,k)
            if row: out[k]=json.loads(row.decision_json)
    missing=[(i,p) for i,p in enumerate(packages) if keys[i] not in out]
    if missing:
        api_key=os.getenv("AI_API_KEY")
        if not api_key:
            raise HTTPException(503, detail={"code":"AI_NOT_CONFIGURED","message":"Decision service is not configured"})
        try:
            from openai import OpenAI
            client=OpenAI(api_key=api_key, base_url=os.getenv("AI_BASE_URL","https://generativelanguage.googleapis.com/v1beta/openai/"), timeout=30.0, max_retries=1)
            payload=[{"index":i,"package":p} for i,p in missing]
            prompt="""You are an evidence-grounded accounts-payable decision engine. For EACH provided synthetic invoice package, read the actual documents and choose exactly one action: settle_invoice, request_approval, hold_invoice, reject_duplicate, open_exception. Never decide from cover sheets, archive examples, training decoys, or irrelevant mentions. A duplicate means the SAME commercial invoice was already paid; same vendor or same amount alone is not duplicate. settle_invoice only if valid, reconciled and within delegated autonomous authority. request_approval if commercially valid but exceeds delegated authority. hold_invoice if payment must pause pending a stated verification. reject_duplicate only for same commercial invoice already paid. open_exception if material records conflict. Return JSON object {\"decisions\":[{\"index\": integer,\"action\": one exact action,\"vendorName\": string,\"invoiceNumber\": string,\"amountMinor\": integer,\"currency\": string,\"evidenceRefs\":[exactly 3 refs],\"rationale\": string 60-1500 chars}]}. Evidence refs must be the three decisive bracketed references from the paragraph that determines the action, copied exactly. Do not invent refs. Rationale names the action and cites at least two refs. amountMinor is amount in minor units (paise for INR), not major currency units. If unclear, choose open_exception and explain conflict. Output only JSON."""
            response=client.chat.completions.create(model=os.getenv("AI_MODEL","gemini-3.8-flash"),messages=[{"role":"system","content":prompt},{"role":"user","content":canon(payload)}],response_format={"type":"json_object"},temperature=0)
            data=json.loads(response.choices[0].message.content or "{}")
            decisions=data.get("decisions",[])
            by_index={d.get("index"):d for d in decisions if isinstance(d,dict)}
            for i,p in missing:
                d=by_index.get(i)
                if not d: raise ValueError("model omitted package decision")
                if d.get("action") not in ACTIONS: raise ValueError("invalid action")
                refs=d.get("evidenceRefs")
                if not isinstance(refs,list) or len(refs)!=3 or any(not isinstance(r,str) or not r.startswith("[") or not r.endswith("]") for r in refs):
                    # Do not fabricate evidence; fail safely if the model can't identify exact refs.
                    raise ValueError("invalid evidence references")
                rationale=str(d.get("rationale", ""))
                if not 60 <= len(rationale) <= 1500 or sum(1 for r in refs if r in rationale) < 2:
                    raise ValueError("invalid rationale")
                amt=d.get("amountMinor")
                if isinstance(amt,bool) or not isinstance(amt,int): raise ValueError("invalid amountMinor")
                clean={"action":d["action"],"vendorName":str(d.get("vendorName","")),"invoiceNumber":str(d.get("invoiceNumber","")),"amountMinor":amt,"currency":str(d.get("currency","INR")),"evidenceRefs":refs,"rationale":rationale}
                out[keys[i]]=clean
            with Session() as db:
                for i,p in missing:
                    db.merge(DecisionCache(cache_key=keys[i],decision_json=canon(out[keys[i]])))
                db.commit()
        except HTTPException: raise
        except Exception:
            # Log the actual exception in Render logs; keep the public response generic.
            # Never return provider keys, invoice contents, or internal tracebacks to the caller.
            logger.exception("Invoice decision generation or validation failed")
            raise HTTPException(502, detail={"code":"DECISION_VALIDATION_FAILED","message":"Invoice decisions could not be validated"})
    return [out[k] for k in keys]

def task_copy(row): return json.loads(row.task_json)
def save_task(db, row, task):
    row.task_json=canon(task); row.state=task["status"]["state"]; row.updated_at=datetime.now(timezone.utc); db.add(row)

def task_artifact(media, data):
    return {"artifactId":make_id("artifact"),"parts":[{"mediaType":media,"data":data}]}

def task_status(state): return {"state":state}
def role_user_message(message):
    m=dict(message); m["role"]="ROLE_USER"; return m

def validate_message(message):
    if not isinstance(message,dict) or not message.get("messageId") or message.get("role") not in ("ROLE_USER","user") or not isinstance(message.get("parts"),list):
        raise HTTPException(400, detail={"code":"INVALID_MESSAGE","message":"A valid user message is required"})

@app.post("/a2a/message:send")
async def send_message(request: Request, authorization: Optional[str]=Header(None), a2a_version: Optional[str]=Header(None, alias="A2A-Version"), content_type: Optional[str]=Header(None, alias="Content-Type")):
    principal=auth_and_protocol(request,authorization,a2a_version,content_type)
    try: body=await request.json()
    except Exception: raise HTTPException(400, detail={"code":"INVALID_JSON","message":"Request body must be JSON"})
    message=body.get("message") if isinstance(body,dict) else None
    validate_message(message)
    message=role_user_message(message)
    message_id=str(message["messageId"]); message_hash=digest(message)
    with lock:
        with Session() as db:
            existing=db.get(Idempotency,(principal,message_id))
            if existing:
                if existing.message_hash != message_hash: return err(409,"IDEMPOTENCY_CONFLICT","Message ID was already used with different content")
                row=db.get(StoredTask,existing.task_id)
                if row: return {"task":task_copy(row)}
                return err(500,"STORAGE_ERROR","Stored task is unavailable")
        initial=get_message_part(message,INPUT_MODE)
        continuation=get_message_part(message,RESULT_MODE)
        if (initial is None) == (continuation is None):
            raise HTTPException(400, detail={"code":"INVALID_MESSAGE_PART","message":"Provide exactly one supported invoice batch or results part"})
        if initial is not None:
            batch_id=initial.get("batchId"); packages=initial.get("packages")
            if not isinstance(batch_id,str) or not batch_id or not isinstance(packages,list) or not packages:
                raise HTTPException(400, detail={"code":"INVALID_BATCH","message":"Batch must include batchId and packages"})
            package_ids=[p.get("packageId") if isinstance(p,dict) else None for p in packages]
            if any(not isinstance(x,str) or not x for x in package_ids) or len(set(package_ids)) != len(package_ids):
                raise HTTPException(400, detail={"code":"INVALID_PACKAGES","message":"Package IDs must be present and unique"})
            decisions=model_decisions(packages)
            proposals=[]
            for p,d in zip(packages,decisions):
                facts={"vendorName":d["vendorName"],"invoiceNumber":d["invoiceNumber"],"amountMinor":d["amountMinor"],"currency":d["currency"]}
                proposals.append({"packageId":p["packageId"],"actionId":make_id("action"),"action":d["action"],"facts":facts,"evidenceRefs":d["evidenceRefs"],"rationale":d["rationale"]})
            if len({x["actionId"] for x in proposals}) != len(proposals): raise HTTPException(500,detail={"code":"ACTION_ID_ERROR","message":"Could not create unique action IDs"})
            task_id=make_id("task"); context_id=str(message.get("contextId") or make_id("context"))
            task={"id":task_id,"contextId":context_id,"status":task_status("TASK_STATE_INPUT_REQUIRED"),"history":[message],"artifacts":[task_artifact(PROPOSAL_MODE,{"batchId":batch_id,"proposals":proposals})],"metadata":{"batchId":batch_id}}
            with Session() as db:
                row=StoredTask(task_id=task_id,principal=principal,context_id=context_id,state="TASK_STATE_INPUT_REQUIRED",task_json=canon(task),batch_id=batch_id,package_cache_keys=canon([digest(p) for p in packages]),created_at=datetime.now(timezone.utc),updated_at=datetime.now(timezone.utc))
                db.add(row); db.add(Idempotency(principal=principal,message_id=message_id,message_hash=message_hash,task_id=task_id))
                try: db.commit()
                except IntegrityError:
                    db.rollback(); existing=db.get(Idempotency,(principal,message_id))
                    if existing and existing.message_hash==message_hash:
                        row=db.get(StoredTask,existing.task_id); return {"task":task_copy(row)}
                    return err(409,"IDEMPOTENCY_CONFLICT","Message ID was already used")
            return {"task":task}
        # Continuation: bind it to the existing task and exact stored proposal.
        task_id=message.get("taskId"); context_id=message.get("contextId")
        if not task_id or not context_id: raise HTTPException(400, detail={"code":"INVALID_CONTINUATION","message":"taskId and contextId are required"})
        with Session() as db:
            row=db.get(StoredTask,str(task_id))
            if not row or row.principal != principal: return err(404,"TASK_NOT_FOUND","Task not found")
            task=task_copy(row)
            if row.context_id != context_id: return err(409,"CONTEXT_MISMATCH","Continuation context does not match")
            if row.state in ("TASK_STATE_COMPLETED","TASK_STATE_CANCELED","TASK_STATE_FAILED"): return err(409,"TASK_TERMINAL","Task is terminal")
            pdata=task["artifacts"][0]["parts"][0]["data"]
            if continuation.get("batchId") != pdata.get("batchId") or row.batch_id != continuation.get("batchId"):
                return err(409,"BATCH_MISMATCH","Continuation batch does not match")
            results=continuation.get("results")
            if not isinstance(results,list): return err(400,"INVALID_RESULTS","Results must be a list")
            proposals={p["packageId"]:p for p in pdata["proposals"]}
            if len({r.get("packageId") for r in results if isinstance(r,dict)}) != len(results): return err(400,"INVALID_RESULTS","Each package may appear only once")
            if {r.get("packageId") for r in results if isinstance(r,dict)} != set(proposals): return err(409,"RESULT_SET_MISMATCH","Results must cover every stored proposal exactly once")
            accepted=[]
            for result in results:
                if not isinstance(result,dict): return err(400,"INVALID_RESULTS","Invalid result entry")
                prop=proposals.get(result.get("packageId"))
                if not prop or result.get("actionId") != prop["actionId"] or result.get("action") != prop["action"] or result.get("outcome") not in ("ACCEPTED","REJECTED"):
                    return err(409,"PROPOSAL_MISMATCH","Result does not match a stored proposal")
                if result.get("outcome")=="ACCEPTED":
                    nonce=result.get("receiptNonce")
                    if not isinstance(nonce,str) or not nonce: return err(409,"RECEIPT_MISMATCH","Accepted result requires its receipt nonce")
                    accepted.append({"packageId":prop["packageId"],"actionId":prop["actionId"],"action":prop["action"],"receiptNonce":nonce,"facts":prop["facts"],"evidenceRefs":prop["evidenceRefs"]})
            # All validation and terminal mutation happen under the process lock and DB transaction.
            task["history"].append(message)
            if len(accepted): task["artifacts"].append(task_artifact(RECEIPT_MODE,{"batchId":row.batch_id,"executions":accepted}))
            task["status"]={"state":"TASK_STATE_COMPLETED"}
            save_task(db,row,task)
            db.add(Idempotency(principal=principal,message_id=message_id,message_hash=message_hash,task_id=task_id))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                existing=db.get(Idempotency,(principal,message_id))
                if existing and existing.message_hash==message_hash:
                    saved=db.get(StoredTask,existing.task_id)
                    if saved: return {"task":task_copy(saved)}
                return err(409,"IDEMPOTENCY_CONFLICT","Message ID was already used")
            return {"task":task}

@app.get("/a2a/tasks")
def list_tasks(request: Request, authorization: Optional[str]=Header(None), a2a_version: Optional[str]=Header(None, alias="A2A-Version")):
    principal=auth_and_protocol(request,authorization,a2a_version,None,False)
    with Session() as db:
        rows=db.execute(select(StoredTask).where(StoredTask.principal==principal).order_by(StoredTask.created_at.desc()).limit(100)).scalars().all()
        return {"tasks":[task_copy(r) for r in rows]}

@app.get("/a2a/tasks/{task_id}")
def get_task(task_id: str, request: Request, authorization: Optional[str]=Header(None), a2a_version: Optional[str]=Header(None, alias="A2A-Version")):
    principal=auth_and_protocol(request,authorization,a2a_version,None,False)
    with Session() as db:
        row=db.get(StoredTask,task_id)
        if not row or row.principal != principal: return err(404,"TASK_NOT_FOUND","Task not found")
        return task_copy(row)

@app.post("/a2a/tasks/{task_id}:cancel")
def cancel_task(task_id: str, request: Request, authorization: Optional[str]=Header(None), a2a_version: Optional[str]=Header(None, alias="A2A-Version"), content_type: Optional[str]=Header(None, alias="Content-Type")):
    principal=auth_and_protocol(request,authorization,a2a_version,content_type)
    with lock, Session() as db:
        row=db.get(StoredTask,task_id)
        if not row or row.principal != principal: return err(404,"TASK_NOT_FOUND","Task not found")
        if row.state in ("TASK_STATE_COMPLETED","TASK_STATE_CANCELED","TASK_STATE_FAILED"): return err(409,"TASK_TERMINAL","Task is terminal")
        task=task_copy(row); task["status"]={"state":"TASK_STATE_CANCELED"}; save_task(db,row,task); db.commit(); return task
