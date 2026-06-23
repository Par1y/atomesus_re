import asyncio
import json
import random
import string
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Security
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.background import BackgroundTask

from utils.Client import Client

router = APIRouter()
security = HTTPBearer()

ATOMESUS_BASE_URL = "https://api.atomesus.com"


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
            parts.append(f"[{role}]: {content}")
    return "\n".join(parts)


async def parse_atomesus_sse(response, stream: bool):
    last_content = ""
    full_text = ""
    finish_reason = None

    async for line in response.aiter_lines():
        line = line.strip()
        if not line:
            continue
        if not line.startswith("data: "):
            continue
        data_str = line[6:].strip()
        if data_str == "[DONE]":
            break
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            continue

        event_type = data.get("type")

        if event_type == "content":
            content = data.get("content", "")
            if not isinstance(content, str):
                content = str(content)
            full_text = content
            if stream:
                delta = content[len(last_content):]
                last_content = content
                if delta:
                    yield {"type": "delta", "content": delta}
            else:
                yield {"type": "partial", "content": content}

        elif event_type == "end":
            reply = data.get("reply", "")
            if reply:
                full_text = reply
            finish_reason = "stop"
            yield {"type": "end", "content": full_text, "finish_reason": finish_reason}
            break

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

    request_id = str(uuid.uuid4())

    headers = {
        "accept": "text/event-stream",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "multipart/form-data; boundary=----WebKitFormBoundary0AR6ESGWHBQmUoAk",
        "origin": "https://www.atomesus.com",
        "referer": "https://www.atomesus.com/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "authorization": f"Bearer {token}",
    }

    boundary = "----WebKitFormBoundary0AR6ESGWHBQmUoAk"
    parts = []
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"message\"\r\n\r\n{message_text}")
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"stream\"\r\n\r\n{str(stream).lower()}")
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"id\"\r\n\r\n{request_id}")
    parts.append(f"--{boundary}--\r\n")
    raw_body = "\r\n".join(parts).encode("utf-8")

    client = Client()
    try:
        resp = await client.post(
            f"{ATOMESUS_BASE_URL}/api/chat/atomesus",
            headers=headers,
            data=raw_body,
            timeout=120,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {str(e)}")

    if resp.status_code != 200:
        text = await resp.atext()
        raise HTTPException(status_code=resp.status_code, detail=text[:500])

    content_type = resp.headers.get("Content-Type", "")
    if "text/event-stream" not in content_type:
        text = await resp.atext()
        raise HTTPException(status_code=502, detail=f"Unexpected content-type: {content_type}, body: {text[:200]}")

    if stream:

        async def stream_generator():
            chat_id = f"chatcmpl-{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(29))}"
            try:
                async for event in parse_atomesus_sse(resp, stream=True):
                    if event["type"] == "delta":
                        chunk = build_openai_chunk(model, content=event["content"], chat_id=chat_id)
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            except Exception:
                pass
            final_chunk = build_openai_chunk(model, content="", finish_reason="stop", chat_id=chat_id)
            yield f"data: {json.dumps(final_chunk, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(stream_generator(), media_type="text/event-stream",
                                 background=BackgroundTask(client.close))
    else:

        full_text = ""
        try:
            async for event in parse_atomesus_sse(resp, stream=False):
                if event["type"] == "end":
                    full_text = event.get("content", "")
                    break
        except Exception:
            pass
        finally:
            await client.close()

        chat_id = f"chatcmpl-{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(29))}"
        data = {
            "id": chat_id,
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
