# coding: utf-8
"""Local lab runner for browser_agent on BrowserUseDriver."""

from __future__ import annotations

import asyncio
import os
import sys

from openjiuwen.core.foundation.llm import init_model
from openjiuwen.core.runner import Runner
from openjiuwen.harness.subagents import create_browser_agent
from openjiuwen.harness.tools.browser_move.utils.env import load_repo_dotenv


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return default


async def main() -> None:
    load_repo_dotenv()

    api_key = _first_env("OPENJIUWEN_API_KEY", "API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY")
    if not api_key:
        raise SystemExit(
            "Missing API key. Set OPENJIUWEN_API_KEY (or API_KEY) in "
            "C:\\Users\\olegs\\agent-core\\.env"
        )

    provider = _first_env("OPENJIUWEN_PROVIDER", "MODEL_PROVIDER", default="OpenAI")
    model_name = _first_env("OPENJIUWEN_MODEL", "MODEL_NAME", default="gpt-4o")
    api_base = _first_env(
        "OPENJIUWEN_API_BASE",
        "API_BASE",
        "OPENROUTER_BASE_URL",
        default="https://api.openai.com/v1",
    )
    query = _first_env(
        "BU_QUERY",
        default="Open https://example.com and report the exact H1 text only.",
    )
    cdp = _first_env("BROWSER_CDP_URL", "PLAYWRIGHT_CDP_URL")

    print(f"provider={provider!r} model={model_name!r}")
    print(f"api_base={api_base!r}")
    print(f"BROWSER_CDP_URL={cdp!r}")
    print(f"query={query!r}")

    model = init_model(
        provider=provider,
        model_name=model_name,
        api_key=api_key,
        api_base=api_base,
        verify_ssl=False,
    )

    agent = create_browser_agent(model, language="en")
    await Runner.start()
    try:
        await agent.ensure_initialized()
        result = await Runner.run_agent(
            agent,
            {"query": query, "conversation_id": "bu-lab-10"},
        )
    finally:
        await Runner.stop()

    print(result)


if __name__ == "__main__":
    asyncio.run(main())