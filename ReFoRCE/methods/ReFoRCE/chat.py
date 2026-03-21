import sys
import os
import re
import random
import time
import json
import threading
from abc import ABC, abstractmethod
from utils import extract_all_blocks


_USAGE_LOCK = threading.Lock()
_USAGE_FILE = None
_USAGE_STATS = {
    "total": {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    "per_test": {}
}


def _new_usage_bucket():
    return {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


def _merge_bucket(dst, src):
    dst["requests"] += int(src.get("requests", 0))
    dst["prompt_tokens"] += int(src.get("prompt_tokens", 0))
    dst["completion_tokens"] += int(src.get("completion_tokens", 0))
    dst["total_tokens"] += int(src.get("total_tokens", 0))


def _persist_usage_locked():
    if not _USAGE_FILE:
        return
    os.makedirs(os.path.dirname(_USAGE_FILE), exist_ok=True)
    with open(_USAGE_FILE, "w") as f:
        json.dump(_USAGE_STATS, f, indent=2)


def init_usage_tracker(output_path: str):
    global _USAGE_FILE, _USAGE_STATS
    usage_file = os.path.join(output_path, "token_usage_summary.json")
    with _USAGE_LOCK:
        _USAGE_FILE = usage_file
        if os.path.exists(usage_file):
            with open(usage_file) as f:
                loaded = json.load(f)
            total = loaded.get("total", {})
            per_test = loaded.get("per_test", {})
            _USAGE_STATS = {
                "total": {
                    "requests": int(total.get("requests", 0)),
                    "prompt_tokens": int(total.get("prompt_tokens", 0)),
                    "completion_tokens": int(total.get("completion_tokens", 0)),
                    "total_tokens": int(total.get("total_tokens", 0)),
                },
                "per_test": {}
            }
            for test_id, bucket in per_test.items():
                _USAGE_STATS["per_test"][test_id] = {
                    "requests": int(bucket.get("requests", 0)),
                    "prompt_tokens": int(bucket.get("prompt_tokens", 0)),
                    "completion_tokens": int(bucket.get("completion_tokens", 0)),
                    "total_tokens": int(bucket.get("total_tokens", 0)),
                }
        else:
            _USAGE_STATS = {
                "total": _new_usage_bucket(),
                "per_test": {}
            }


def record_token_usage(test_id: str, prompt_tokens: int, completion_tokens: int, total_tokens: int):
    with _USAGE_LOCK:
        key = test_id if test_id else "unknown"
        if key not in _USAGE_STATS["per_test"]:
            _USAGE_STATS["per_test"][key] = _new_usage_bucket()

        delta = {
            "requests": 1,
            "prompt_tokens": int(prompt_tokens),
            "completion_tokens": int(completion_tokens),
            "total_tokens": int(total_tokens),
        }
        _merge_bucket(_USAGE_STATS["per_test"][key], delta)
        _merge_bucket(_USAGE_STATS["total"], delta)
        _persist_usage_locked()


def dump_usage_summary():
    with _USAGE_LOCK:
        _persist_usage_locked()
        print("==================== TOKEN USAGE SUMMARY ====================")
        print("[TokenUsage] Per candidate")
        for test_id in sorted(_USAGE_STATS["per_test"].keys()):
            bucket = _USAGE_STATS["per_test"][test_id]
            print(
                f"[TokenUsage][{test_id}] requests={bucket['requests']}, "
                f"prompt={bucket['prompt_tokens']}, completion={bucket['completion_tokens']}, total={bucket['total_tokens']}"
            )

        per_test_agg = {}
        for tracker_key, bucket in _USAGE_STATS["per_test"].items():
            test_id = tracker_key.split("@", 1)[0]
            if test_id not in per_test_agg:
                per_test_agg[test_id] = _new_usage_bucket()
            _merge_bucket(per_test_agg[test_id], bucket)

        print("[TokenUsage] Per test (aggregated over candidates)")
        for test_id in sorted(per_test_agg.keys()):
            bucket = per_test_agg[test_id]
            print(
                f"[TokenUsage][{test_id}] requests={bucket['requests']}, "
                f"prompt={bucket['prompt_tokens']}, completion={bucket['completion_tokens']}, total={bucket['total_tokens']}"
            )

        total = _USAGE_STATS["total"]
        print(
            f"[TokenUsage][TOTAL] requests={total['requests']}, "
            f"prompt={total['prompt_tokens']}, completion={total['completion_tokens']}, total={total['total_tokens']}"
        )
        if _USAGE_FILE:
            print(f"[TokenUsage] saved to {_USAGE_FILE}")
        print("=============================================================")

class BaseChat(ABC):
    def __init__(self, model: str, temperature: float = 1.0):
        self.model = model
        self.temperature = float(temperature)
        self.messages = []

    @abstractmethod
    def get_response(self, prompt) -> str:
        pass

    def get_model_response(self, prompt, code_format=None) -> list:
        code_blocks = []
        max_try = int(os.environ.get("OPENAI_RETRY_MAX", "8"))
        base_wait = float(os.environ.get("OPENAI_RETRY_BASE_SEC", "1.0"))
        while code_blocks == [] and max_try > 0:
            max_try -= 1
            try:
                response = self.get_response(prompt)
            except Exception as e:
                print(f"max_try: {max_try}, exception: {e}")
                wait_sec = self._get_retry_wait_seconds(e, base_wait, max_try)
                if wait_sec > 0 and max_try > 0:
                    print(f"Rate-limit/backoff sleep: {wait_sec:.2f}s")
                    time.sleep(wait_sec)
                continue
            code_blocks = extract_all_blocks(response, code_format)
        if max_try == 0 or code_blocks == []:
            print(f"get_model_response() exit, max_try: {max_try}, code_blocks: {code_blocks}")
            sys.exit(0)
        return code_blocks

    def get_model_response_txt(self, prompt) -> str:
        max_try = int(os.environ.get("OPENAI_RETRY_MAX", "8"))
        base_wait = float(os.environ.get("OPENAI_RETRY_BASE_SEC", "1.0"))
        while max_try > 0:
            max_try -= 1
            try:
                response = self.get_response(prompt)
                return response
            except Exception as e:
                print(f"max_try: {max_try}, exception: {e}")
                wait_sec = self._get_retry_wait_seconds(e, base_wait, max_try)
                if wait_sec > 0 and max_try > 0:
                    print(f"Rate-limit/backoff sleep: {wait_sec:.2f}s")
                    time.sleep(wait_sec)
                continue
        print(f"get_model_response_txt() exit, max_try: {max_try}")
        sys.exit(0)

    def _get_retry_wait_seconds(self, error, base_wait: float, remaining_tries: int) -> float:
        message = str(error)
        retry_match = re.search(r"try again in\s*([0-9]+)\s*ms", message, re.IGNORECASE)
        if retry_match:
            retry_wait = int(retry_match.group(1)) / 1000.0
        else:
            retry_wait = 0.0

        is_rate_limit = (
            "429" in message
            or "rate limit" in message.lower()
            or "rate_limit_exceeded" in message.lower()
        )

        exp_step = max(0, int(os.environ.get("OPENAI_RETRY_MAX", "8")) - remaining_tries - 1)
        exp_wait = base_wait * (2 ** exp_step)
        jitter = random.uniform(0, 0.5)

        wait_floor = 3.0 if is_rate_limit else 0.0
        wait_sec = max(retry_wait, exp_wait, wait_floor) + jitter
        return min(wait_sec, 30.0)

    def get_message_len(self):
        return {
            "prompt_len": sum(len(item["content"]) for item in self.messages if item["role"] == "user"),
            "response_len": sum(len(item["content"]) for item in self.messages if item["role"] == "assistant"),
            "num_calls": len(self.messages) // 2
        }

    def init_messages(self):
        self.messages = []


from openai import OpenAI, AzureOpenAI

class GPTChat(BaseChat):
    def __init__(self, azure=False, model="gpt-4o", temperature=1.0, tracker_key=None):
        super().__init__(model, temperature)
        self.tracker_key = tracker_key

        if not azure:
            if model in ["o1-preview", "o1-mini"]:
                self.client = OpenAI(
                    api_key=os.environ.get("OPENAI_API_KEY"),
                    api_version="2024-12-01-preview"
                )
            elif model in ["deepseek-reasoner"]:
                self.client = OpenAI(
                    base_url="https://api.deepseek.com",
                    api_key=os.environ.get("DS_API_KEY"),
                )
            else:
                self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        else:
            if model in ["o1-preview", "o1-mini", "o3", "o4-mini"]:
                version = "2024-12-01-preview"
            elif model in ["o3-pro"]:
                version = "2025-03-01-preview"
            else:
                version = "2024-05-01-preview"

            self.client = AzureOpenAI(
                azure_endpoint=os.environ.get("AZURE_ENDPOINT"),
                api_key=os.environ.get("AZURE_OPENAI_KEY"),
                api_version=version
            )

    def get_response(self, prompt) -> str:
        self.messages.append({"role": "user", "content": prompt})
        if self.model == "o3-pro":
            response = self.client.responses.create(
                model=self.model,
                input=self.messages,
                temperature=self.temperature
            )
            main_content = response.output_text
        else:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                temperature=self.temperature
            )
            main_content = response.choices[0].message.content

        prompt_tokens, completion_tokens, total_tokens = self._extract_usage(response)
        record_token_usage(self.tracker_key, prompt_tokens, completion_tokens, total_tokens)

        self.messages.append({"role": "assistant", "content": main_content})
        return main_content

    def _extract_usage(self, response):
        usage = getattr(response, "usage", None)
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0

        if usage is not None:
            prompt_tokens = getattr(usage, "prompt_tokens", None)
            if prompt_tokens is None:
                prompt_tokens = getattr(usage, "input_tokens", 0)

            completion_tokens = getattr(usage, "completion_tokens", None)
            if completion_tokens is None:
                completion_tokens = getattr(usage, "output_tokens", 0)

            total_tokens = getattr(usage, "total_tokens", None)
            if total_tokens is None:
                total_tokens = int(prompt_tokens or 0) + int(completion_tokens or 0)

        return int(prompt_tokens or 0), int(completion_tokens or 0), int(total_tokens or 0)