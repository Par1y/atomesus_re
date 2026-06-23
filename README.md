# atomesus_re

# 工作逻辑：

接收 OpenAI 格式的 POST /v1/chat/completions（Bearer Token 鉴权）
将 messages 数组拼接为单文本，构建 multipart/form-data 请求体
转发到 https://api.atomesus.com/api/chat/atomesus
解析 atomesus SSE 响应：
type: content 的 content 字段是累积文本，通过对比上次长度提取增量 delta
type: end 的 reply 字段包含完整最终回复
流式模式：逐 delta 转换为 OpenAI chat.completion.chunk SSE 格式
非流式模式：收集完整文本后一次性返回 OpenAI chat.completion JSON

# 启动：

```sh
cd atomesus_re
pip install -r requirements.txt
python app.py
```

# 调用示例：

```sh
curl -X POST http://127.0.0.1:5005/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <你的atomesus_token>' \
  -d '{
    "model": "atomesus",
    "messages": [{"role": "user", "content": "hello"}],
    "stream": true
  }'
```
