# coding: utf-8
"""Local lab runner for browser_agent on BrowserUseDriver."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional, Sequence, Tuple
from uuid import uuid4

from openjiuwen.core.foundation.llm import init_model
from openjiuwen.core.runner import Runner
from openjiuwen.harness.subagents import create_browser_agent
from openjiuwen.harness.tools.browser_move.shared.env import load_repo_dotenv

DEFAULT_QUERY = "Open https://example.com and report the exact H1 text only."

# Query sources, highest precedence first. cmd.exe mangles & | ^ < > and quotes
# inside environment variables, so a file path is the only reliable way to hand
# a non-trivial prompt to this runner on Windows.
QUERY_SOURCE_ARGV_FILE = "argv-file"
QUERY_SOURCE_ARGV = "argv"
QUERY_SOURCE_ENV_FILE = "env-file"
QUERY_SOURCE_ENV = "env"
QUERY_SOURCE_DEFAULT = "default"


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return default


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run browser_agent against a local BrowserUse session.")
    parser.add_argument("--query", default=None, help="Prompt text.")
    parser.add_argument("--query-file", default=None, help="Path to a UTF-8 file holding the prompt.")
    parser.add_argument("--conversation-id", default=None, help="Session id for the run.")
    return parser


def read_query_file(path: str, *, origin: str) -> str:
    """Read a prompt file as UTF-8, preserving newlines and special characters."""
    candidate = Path(path).expanduser()
    try:
        return candidate.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"Query file not found ({origin}): {candidate}") from exc
    except OSError as exc:
        raise SystemExit(f"Cannot read query file ({origin}): {candidate}: {exc}") from exc


def resolve_query(
    args: argparse.Namespace,
    env: Optional[Mapping[str, str]] = None,
) -> Tuple[str, str]:
    """Return ``(query, source)`` for the first non-empty source in precedence order."""
    environ = os.environ if env is None else env

    argv_file = (getattr(args, "query_file", None) or "").strip()
    if argv_file:
        return read_query_file(argv_file, origin="--query-file"), QUERY_SOURCE_ARGV_FILE

    argv_query = getattr(args, "query", None) or ""
    if argv_query.strip():
        return argv_query, QUERY_SOURCE_ARGV

    env_file = (environ.get("BU_QUERY_FILE") or "").strip()
    if env_file:
        return read_query_file(env_file, origin="BU_QUERY_FILE"), QUERY_SOURCE_ENV_FILE

    env_query = environ.get("BU_QUERY") or ""
    if env_query.strip():
        return env_query, QUERY_SOURCE_ENV

    return DEFAULT_QUERY, QUERY_SOURCE_DEFAULT


def resolve_conversation_id(
    args: argparse.Namespace,
    env: Optional[Mapping[str, str]] = None,
) -> str:
    """Return the session id, defaulting to a timestamp so runs never share phase state."""
    environ = os.environ if env is None else env

    argv_id = (getattr(args, "conversation_id", None) or "").strip()
    if argv_id:
        return argv_id
    env_id = (environ.get("BU_CONVERSATION_ID") or "").strip()
    if env_id:
        return env_id
    # The random tail is not decoration: the wall clock is too coarse to keep
    # two back-to-back runs apart, and a shared id means shared phase state.
    return f"bu-lab-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid4().hex[:6]}"


async def main(argv: Optional[Sequence[str]] = None) -> None:
    load_repo_dotenv()

    args = build_arg_parser().parse_args(sys.argv[1:] if argv is None else list(argv))

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
    query, query_source = resolve_query(args)
    conversation_id = resolve_conversation_id(args)
    cdp = _first_env("BROWSER_CDP_URL", "PLAYWRIGHT_CDP_URL")

    print(f"provider={provider!r} model={model_name!r}")
    print(f"api_base={api_base!r}")
    print(f"BROWSER_CDP_URL={cdp!r}")
    print(f"query_source={query_source} query_chars={len(query)} conversation_id={conversation_id!r}")
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
            {"query": query, "conversation_id": conversation_id},
        )
    finally:
        await Runner.stop()

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
