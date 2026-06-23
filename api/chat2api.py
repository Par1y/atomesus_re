import json
import os
import random
import string
import time
import uuid

import fcntl
import jwt

from fastapi import APIRouter, HTTPException, Request, Security
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.background import BackgroundTask

from utils.Client import Client
from utils.config import DEBUG

router = APIRouter()
security = HTTPBearer()

ATOMESUS_BASE_URL = "https://api.atomesus.com"
SESSION_DIR = "data"
SESSION_FILE = os.path.join(SESSION_DIR, "session_map.json")
SESSION_TTL = 6 * 60 * 60
RETRY_TIMES = 3


def _ensure_data_dir():
    if not os.path.exists(SESSION_DIR):
        os.makedirs(SESSION_DIR)


def _load_sessions() -> dict:
    _ensure_data_dir()
    if not os.path.exists(SESSION_FILE):
        return {}
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        now = int(time.time())
        return {ip: entry for ip, entry in data.items()
                if now - entry.get("created_at", 0) < SESSION_TTL}
    except Exception:
        return {}


def _save_sessions(sessions: dict):
    _ensure_data_dir()
    tmp_file = SESSION_FILE + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        json.dump(sessions, f, indent=2)
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    os.replace(tmp_file, SESSION_FILE)


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


def get_or_create_session(request: Request, token: str) -> str:
    client_ip = get_client_ip(request)
    sessions = _load_sessions()
    now = int(time.time())
    entry = sessions.get(client_ip)
    if entry and (now - entry.get("created_at", 0)) < SESSION_TTL:
        return entry["session_id"]
    user_id = "unknown"
    try:
        payload = jwt.decode(token, options={"verify_signature": False})
        user_id = payload.get("id", "unknown")
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] jwt_decode_failed: {type(e).__name__}: {e}")
    session_id = f"chat_{user_id}_{int(time.time() * 1000)}"
    sessions[client_ip] = {"session_id": session_id, "created_at": now}
    _save_sessions(sessions)
    return session_id


def combine_messages(messages: list) -> str:
    parts = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            texts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    texts.append(block.get("text", ""))
            content = " ".join(texts)
        if content:
            parts.append(content)
    return "\n".join(parts)


async def parse_atomesus_sse(response, stream: bool, debug: bool = False):
    full_text = ""
    finish_reason = None
    line_count = 0

    if debug:
        print("[DEBUG] parse_atomesus_sse: starting aiter_lines")

    try:
        async for line in response.aiter_lines():
            line_count += 1
            if isinstance(line, bytes):
                line = line.decode("utf-8", errors="replace")
            line = line.strip()
            if not line:
                continue
            if not line.startswith("data: "):
                if debug:
                    print(f"[DEBUG] raw_sse_skip: {line[:200]}")
                continue
            data_str = line[6:].strip()
            if data_str == "[DONE]":
                if debug:
                    print("[DEBUG] raw_sse: [DONE]")
                break
            if debug:
                print(f"[DEBUG] raw_sse: {data_str[:500]}")
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                if debug:
                    print(f"[DEBUG] raw_sse_json_error: {data_str[:200]}")
                continue

            event_type = data.get("type")

            if event_type == "content":
                content = data.get("content", "")
                if not isinstance(content, str):
                    content = str(content)
                full_text += content
                if stream:
                    if content:
                        yield {"type": "delta", "content": content}
                else:
                    yield {"type": "partial", "content": full_text}

            elif event_type == "end":
                reply = data.get("reply", "")
                if reply:
                    full_text = reply
                finish_reason = "stop"
                yield {"type": "end", "content": full_text, "finish_reason": finish_reason}
                break
            else:
                if debug:
                    print(f"[DEBUG] raw_sse_unknown_type: {event_type} data={data_str[:300]}")
    except Exception as e:
        if debug:
            print(f"[DEBUG] parse_atomesus_sse_exception: {type(e).__name__}: {e} lines_read={line_count}")
        raise

    if debug:
        print(f"[DEBUG] parse_atomesus_sse: done lines={line_count} full_text_len={len(full_text)}")

    if stream and finish_reason is None:
        yield {"type": "end", "content": full_text, "finish_reason": "stop"}


def build_openai_chunk(model: str, content: str = "", finish_reason: Optional[str] = None,
                       chat_id: Optional[str] = None) -> dict:
    chunk = {
        "id": chat_id or f"chatcmpl-{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(29))}",
        "object": "chat.completion.chunk" if finish_reason is None else "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {"role": "assistant", "content": content} if finish_reason is None else {"role": "assistant"},
                "logprobs": None,
                "finish_reason": finish_reason,
            }
        ],
    }
    return chunk


async def _post_with_retry(client: Client, url: str, headers: dict, data: bytes, timeout: int, debug: bool = False):
    last_err = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            resp = await client.post_stream(
                url,
                headers=headers,
                data=data,
                timeout=timeout,
                stream=True,
            )
            if resp.status_code == 429:
                last_err = HTTPException(status_code=429, detail="rate-limit")
                if debug:
                    print(f"[DEBUG] retry {attempt}/{RETRY_TIMES} status=429")
                await client.close()
                continue
            if resp.status_code >= 500:
                last_err = HTTPException(status_code=resp.status_code, detail="upstream_5xx")
                if debug:
                    print(f"[DEBUG] retry {attempt}/{RETRY_TIMES} status={resp.status_code}")
                await client.close()
                continue
            return resp
        except HTTPException:
            raise
        except Exception as e:
            last_err = e
            if debug:
                print(f"[DEBUG] retry {attempt}/{RETRY_TIMES} error={e}")
            await client.close()
            continue
    if isinstance(last_err, HTTPException):
        raise last_err
    raise HTTPException(status_code=502, detail=f"Upstream request failed after {RETRY_TIMES} retries")


@router.post("/v1/chat/completions")
async def chat_completions(request: Request,
                           credentials: HTTPAuthorizationCredentials = Security(security)):
    token = credentials.credentials
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail={"error": "Invalid JSON body"})

    messages = body.get("messages", [])
    model = body.get("model", "gpt-3.5-turbo")
    stream = body.get("stream", False)

    message_text = combine_messages(messages)
    session_id = get_or_create_session(request, token)
    message_id = str(uuid.uuid4())

    if DEBUG:
        print(f"[DEBUG] token_prefix={token[:12]}... session_id={session_id}")
        print(f"[DEBUG] model={model} stream={stream} messages={len(messages)}")
        print(f"[DEBUG] message_text={message_text[:500]}")

    headers = {
        "accept": "text/event-stream",
        "accept-language": request.headers.get("accept-language", "en-US,en;q=0.9"),
        "content-type": "multipart/form-data; boundary=----WebKitFormBoundary0AR6ESGWHBQmUoAk",
        "origin": "https://www.atomesus.com",
        "referer": "https://www.atomesus.com/",
        "user-agent": request.headers.get("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "authorization": f"Bearer {token}",
    }

    boundary = "----WebKitFormBoundary0AR6ESGWHBQmUoAk"
    parts = []
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"message\"\r\n\r\n{message_text}")
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"stream\"\r\n\r\n{str(stream).lower()}")
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"sessionId\"\r\n\r\n{session_id}")
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"id\"\r\n\r\n{message_id}")
    parts.append(f"--{boundary}--\r\n")
    raw_body = "\r\n".join(parts).encode("utf-8")

    client = Client()
    try:
        resp = await _post_with_retry(
            client,
            f"{ATOMESUS_BASE_URL}/api/chat/atomesus",
            headers=headers,
            data=raw_body,
            timeout=120,
            debug=DEBUG,
        )
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] upstream_request_failed: {e}")
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {str(e)}")

    if resp.status_code != 200:
        text = await resp.atext()
        if DEBUG:
            print(f"[DEBUG] upstream_status={resp.status_code} body={text[:300]}")
        raise HTTPException(status_code=resp.status_code, detail=text[:500])

    content_type = resp.headers.get("Content-Type", "")
    if "text/event-stream" not in content_type:
        text = await resp.atext()
        if DEBUG:
            print(f"[DEBUG] unexpected_content_type={content_type} body={text[:200]}")
        raise HTTPException(status_code=502, detail=f"Unexpected content-type: {content_type}, body: {text[:200]}")

    if DEBUG:
        print(f"[DEBUG] upstream_connected stream={stream}")

    if stream:

        async def stream_generator():
            chat_id = f"chatcmpl-{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(29))}"
            try:
                async for event in parse_atomesus_sse(resp, stream=True, debug=DEBUG):
                    if event["type"] == "delta":
                        if DEBUG:
                            print(f"[DEBUG] stream_delta len={len(event['content'])} text={event['content'][:100]}")
                        chunk = build_openai_chunk(model, content=event["content"], chat_id=chat_id)
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            except Exception as e:
                if DEBUG:
                    print(f"[DEBUG] stream_error: {e}")
                err_chunk = {
                    "id": chat_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": f"\n[stream error: {e}]\n"}, "logprobs": None, "finish_reason": "stop"}],
                }
                yield f"data: {json.dumps(err_chunk, ensure_ascii=False)}\n\n"
            final_chunk = build_openai_chunk(model, content="", finish_reason="stop", chat_id=chat_id)
            final_chunk["conversation_id"] = session_id
            yield f"data: {json.dumps(final_chunk, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(stream_generator(), media_type="text/event-stream",
                                 background=BackgroundTask(client.close))
    else:

        full_text = ""
        try:
            async for event in parse_atomesus_sse(resp, stream=False, debug=DEBUG):
                if event["type"] == "end":
                    full_text = event.get("content", "")
                    break
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] non_stream_error: {e}")
            raise HTTPException(status_code=502, detail=f"Upstream stream error: {e}")
        finally:
            await client.close()

        if DEBUG:
            print(f"[DEBUG] non_stream_result len={len(full_text)} text={full_text[:200]}")

        chat_id = f"chatcmpl-{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(29))}"
        data = {
            "id": chat_id,
            "conversation_id": session_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": full_text},
                    "logprobs": None,
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
        return JSONResponse(content=data)
