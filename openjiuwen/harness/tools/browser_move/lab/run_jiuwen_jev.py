# coding: utf-8
"""Local lab runner: the browser subagent with TypeSafe Jev in its model slot (Zurich to London by default)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence
from uuid import uuid4

from openjiuwen.core.foundation.llm import init_model
from openjiuwen.core.runner import Runner
from openjiuwen.harness.subagents import create_browser_agent
from openjiuwen.harness.tools.browser_move.lab.profiler import JevProfiler, render
from openjiuwen.harness.tools.browser_move.policy.jev_decision_model import JevDecisionModel
from openjiuwen.harness.tools.browser_move.policy.jev_decisions import (
    DECISIONS_BACKENDS,
    DECISIONS_TIMEOUT_S,
    client_from_env,
)
from openjiuwen.harness.tools.browser_move.shared.env import load_repo_dotenv

DEFAULT_QUERY = (
    "Open https://www.google.com/travel/flights?hl=en and find one-way flights from Zurich to London on "
    "September 20, 2026, for one adult in economy. Stop when matching flight options are visible."
)


def _first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


async def main(argv: Optional[Sequence[str]] = None) -> None:
    load_repo_dotenv()
    parser = argparse.ArgumentParser(description="Run browser_agent with Jev deciding every browser step.")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--goal-value-cache", action="store_true", help="Offer goal-extracted values to Jev.")
    parser.add_argument("--profile-out", default="", help="Write the profiler report as JSON to this path.")
    parser.add_argument(
        "--decisions",
        choices=DECISIONS_BACKENDS,
        required=True,
        help="Who answers Jev: TypeSafe directly (TYPESAFE_API_KEY) or the OpenRouter proxy (OPENROUTER_API_KEY).",
    )
    parser.add_argument(
        "--prefetch",
        choices=("on", "off"),
        required=True,
        help="Generate typed values ahead of time for every editable field (on), or when TYPE_TEXT is chosen (off).",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else list(argv))

    api_key = _first_env("OPENJIUWEN_API_KEY", "API_KEY", "OPENROUTER_API_KEY", "OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing API key for the chat fallback: set OPENROUTER_API_KEY in .env")
    decisions = client_from_env(args.decisions, timeout_s=DECISIONS_TIMEOUT_S)
    fallback = init_model(
        provider=_first_env("OPENJIUWEN_PROVIDER", "MODEL_PROVIDER") or "openrouter",
        model_name=_first_env("OPENJIUWEN_MODEL", "MODEL_NAME") or "google/gemini-2.5-flash",
        api_key=api_key,
        api_base=_first_env("OPENJIUWEN_API_BASE", "API_BASE", "OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1",
        temperature=0.0,
        verify_ssl=False,
    )
    value_model = None
    if _first_env("JEV_VALUE_MODEL"):  # mercury-2.5 answers empty through the openjiuwen client; keep this explicit
        value_model = init_model(
            provider="openrouter",
            model_name=_first_env("JEV_VALUE_MODEL"),
            api_key=_first_env("TEXT_MODEL_API_KEY") or api_key,
            api_base=_first_env("TEXT_MODEL_BASE_URL") or "https://openrouter.ai/api/v1",
            temperature=0.0,
            verify_ssl=False,
        )
    decider = JevDecisionModel(
        fallback,
        language="en",
        client=decisions,
        goal_value_cache=args.goal_value_cache,
        prefetch_values=args.prefetch == "on",
        value_model=value_model,
    )
    print(f"prefetch: {args.prefetch}")
    print(f"value model: {_first_env('JEV_VALUE_MODEL') or 'chat fallback'}")
    print(f"decisions: {decider._decisions.model} via {decider._decisions.url}")

    profiler = JevProfiler().attach()
    agent = create_browser_agent(decider, language="en")
    conversation_id = f"jiuwen-jev-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid4().hex[:6]}"
    await Runner.start()
    try:
        await agent.ensure_initialized()
        result = await Runner.run_agent(agent, {"query": args.query, "conversation_id": conversation_id})
    finally:
        await Runner.stop()
        profiler.detach()

    for tick in decider.ticks:
        print(json.dumps(tick, ensure_ascii=False))
    print("result:", result)
    print("report:", json.dumps(decider.report(), ensure_ascii=False, indent=1))
    profile = profiler.report(decider)
    print(render(profile))
    if args.profile_out:
        await asyncio.to_thread(
            Path(args.profile_out).write_text, json.dumps(profile, ensure_ascii=False, indent=1), encoding="utf-8"
        )


if __name__ == "__main__":
    asyncio.run(main())
