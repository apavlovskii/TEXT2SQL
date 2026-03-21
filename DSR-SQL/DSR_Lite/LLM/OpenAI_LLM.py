import time
import json
import os
from openai import OpenAI


def _load_openai_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "LLM_config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found. Please ensure the {config_path} file exists.")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    gpt_config = config.get("GPT", {})
    url = gpt_config.get("url") or "https://api.openai.com/v1"
    key = gpt_config.get("key") or os.getenv("OPENAI_API_KEY")
    model = gpt_config.get("model") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"

    if not key:
        raise ValueError("OpenAI API key is missing. Set GPT.key in LLM_config.json or OPENAI_API_KEY.")

    return {"url": url, "key": key, "model": model}


def _load_openai_client_from_config():
    cfg = _load_openai_config()
    return OpenAI(api_key=cfg["key"], base_url=cfg["url"])


def _resolve_openai_model(requested_model=None):
    cfg = _load_openai_config()
    if requested_model in [None, "", "default"]:
        return cfg["model"]
    return requested_model


def openai_Think(messages, temperature=1, model=None, max_retries=3, max_token=65535):
    """
    OpenAI-backed function matching the project's Think return format:
    (input_token_count, output_token_count, reasoning_content, content)
    """
    try:
        client = _load_openai_client_from_config()
        resolved_model = _resolve_openai_model(model)
    except Exception as e:
        error_message = f"LLM configuration error: {str(e)}"
        print(error_message)
        return 0, 0, "", error_message

    attempt = 0
    token_data = {"prompt_tokens": 0, "completion_tokens": 0}

    while attempt < max_retries:
        try:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_token,
                stream=False
            )

            token_data["prompt_tokens"] = getattr(response.usage, "prompt_tokens", 0) or 0
            token_data["completion_tokens"] = getattr(response.usage, "completion_tokens", 0) or 0

            content = response.choices[0].message.content or ""
            reasoning_content = ""

            return token_data["prompt_tokens"], token_data["completion_tokens"], reasoning_content, content

        except Exception as e:
            print(f"['OpenAI Think' model - Attempt {attempt + 1}] LLM call exception: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                time.sleep(1 * attempt)

    return token_data["prompt_tokens"], token_data["completion_tokens"], "", "The LLM call still failed after multiple retries."


def openai_chat(messages, temperature=1, model=None, max_retries=3, max_token=8192):
    """
    OpenAI-backed function matching the project's Chat return format:
    (input_token_count, output_token_count, reasoning_content, content)
    """
    try:
        client = _load_openai_client_from_config()
        resolved_model = _resolve_openai_model(model)
    except Exception as e:
        error_message = f"LLM configuration error: {str(e)}"
        print(error_message)
        return 0, 0, "", error_message

    attempt = 0
    token_data = {"prompt_tokens": 0, "completion_tokens": 0}

    while attempt < max_retries:
        try:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_token,
                stream=False
            )

            token_data["prompt_tokens"] = getattr(response.usage, "prompt_tokens", 0) or 0
            token_data["completion_tokens"] = getattr(response.usage, "completion_tokens", 0) or 0

            content = response.choices[0].message.content or ""

            return token_data["prompt_tokens"], token_data["completion_tokens"], "", content

        except Exception as e:
            print(f"['OpenAI Chat' model - Attempt {attempt + 1}] LLM call exception: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                time.sleep(1 * attempt)

    return token_data["prompt_tokens"], token_data["completion_tokens"], "", "The LLM call still failed after multiple retries."
