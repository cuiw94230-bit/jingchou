"""DeepSeek 辅助调用脚本：命令行请求大模型并返回文本结果。"""`n`nfrom __future__ import annotations

import argparse
import json
import os
import sys
from urllib import error, request


API_URL = "https://api.deepseek.com/chat/completions"


def call_deepseek(api_key: str, model: str, prompt: str, system: str) -> str:
    payload = {
        "model": model,
        "temperature": 0.3,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        API_URL,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"].strip()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Call DeepSeek from local terminal.")
    parser.add_argument("--model", default=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"))
    parser.add_argument("--system", default="You are a careful coding assistant. Give actionable guidance first.")
    parser.add_argument("--prompt", help="Prompt text. If omitted, stdin will be used.")
    parser.add_argument("--api-key", default=os.environ.get("DEEPSEEK_API_KEY", ""))
    args = parser.parse_args()

    api_key = args.api_key.strip()
    if not api_key:
        print("Missing DeepSeek API key. Use --api-key or set DEEPSEEK_API_KEY.", file=sys.stderr)
        return 1

    prompt = args.prompt if args.prompt is not None else sys.stdin.read()
    prompt = prompt.strip()
    if not prompt:
        print("Prompt is empty.", file=sys.stderr)
        return 1

    try:
        print(call_deepseek(api_key, args.model, prompt, args.system))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
