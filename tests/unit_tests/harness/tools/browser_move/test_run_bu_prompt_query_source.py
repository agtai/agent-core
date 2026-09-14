#!/usr/bin/env python
# coding: utf-8

"""Query and session-id resolution for the local browser lab runner.

The runner used to read the prompt only from ``BU_QUERY``. On Windows cmd.exe a
prompt containing ``& | ^ < >`` or quotes is truncated or mangled before the
process ever starts, which is how the lab-19 / lab-20 prompts arrived cut off.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from openjiuwen.harness.tools.browser_move.lab.run_bu_prompt import (
    DEFAULT_QUERY,
    build_arg_parser,
    resolve_conversation_id,
    resolve_query,
)

_SPECIAL_QUERY = 'Open https://a.test?x=1&y=2 | grep "H1" ^ report <first> line\nsecond line\n'


def _args(argv: list[str]):
    return build_arg_parser().parse_args(argv)


def test_argv_file_wins_over_every_other_source(tmp_path: Path) -> None:
    argv_file = tmp_path / "argv.txt"
    argv_file.write_text("from argv file", encoding="utf-8")
    env_file = tmp_path / "env.txt"
    env_file.write_text("from env file", encoding="utf-8")
    env = {"BU_QUERY_FILE": str(env_file), "BU_QUERY": "from env"}

    query, source = resolve_query(_args(["--query-file", str(argv_file), "--query", "from argv"]), env)

    assert (query, source) == ("from argv file", "argv-file")


def test_argv_query_wins_over_the_environment(tmp_path: Path) -> None:
    env_file = tmp_path / "env.txt"
    env_file.write_text("from env file", encoding="utf-8")
    env = {"BU_QUERY_FILE": str(env_file), "BU_QUERY": "from env"}

    assert resolve_query(_args(["--query", "from argv"]), env) == ("from argv", "argv")


def test_env_file_wins_over_env_text(tmp_path: Path) -> None:
    env_file = tmp_path / "env.txt"
    env_file.write_text("from env file", encoding="utf-8")
    env = {"BU_QUERY_FILE": str(env_file), "BU_QUERY": "from env"}

    assert resolve_query(_args([]), env) == ("from env file", "env-file")


def test_env_text_is_used_when_no_file_is_given() -> None:
    assert resolve_query(_args([]), {"BU_QUERY": "from env"}) == ("from env", "env")


def test_default_query_is_the_last_resort() -> None:
    assert resolve_query(_args([]), {}) == (DEFAULT_QUERY, "default")


def test_blank_sources_fall_through_to_the_next_one() -> None:
    env = {"BU_QUERY_FILE": "   ", "BU_QUERY": "   "}

    assert resolve_query(_args(["--query", "  "]), env) == (DEFAULT_QUERY, "default")


@pytest.mark.parametrize("flag", ["--query-file", "env"])
def test_special_characters_and_newlines_survive_a_utf8_file(tmp_path: Path, flag: str) -> None:
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text(_SPECIAL_QUERY + "价格 & 排序\n", encoding="utf-8")

    if flag == "env":
        query, source = resolve_query(_args([]), {"BU_QUERY_FILE": str(prompt_file)})
        assert source == "env-file"
    else:
        query, source = resolve_query(_args(["--query-file", str(prompt_file)]), {})
        assert source == "argv-file"

    assert query == _SPECIAL_QUERY + "价格 & 排序\n"
    assert query.count("\n") == 3
    assert "&" in query and "|" in query and "^" in query and "<first>" in query


def test_missing_argv_query_file_exits_naming_the_path(tmp_path: Path) -> None:
    missing = tmp_path / "nope.txt"

    with pytest.raises(SystemExit) as excinfo:
        resolve_query(_args(["--query-file", str(missing)]), {})

    assert str(missing) in str(excinfo.value)
    assert "--query-file" in str(excinfo.value)


def test_missing_env_query_file_exits_naming_the_path(tmp_path: Path) -> None:
    missing = tmp_path / "nope.txt"

    with pytest.raises(SystemExit) as excinfo:
        resolve_query(_args([]), {"BU_QUERY_FILE": str(missing)})

    assert str(missing) in str(excinfo.value)
    assert "BU_QUERY_FILE" in str(excinfo.value)


def test_conversation_id_precedence() -> None:
    env = {"BU_CONVERSATION_ID": "from-env"}

    assert resolve_conversation_id(_args(["--conversation-id", "from-argv"]), env) == "from-argv"
    assert resolve_conversation_id(_args([]), env) == "from-env"


def test_default_conversation_ids_are_distinct_across_consecutive_calls() -> None:
    first = resolve_conversation_id(_args([]), {})
    second = resolve_conversation_id(_args([]), {})

    assert first.startswith("bu-lab-")
    assert second.startswith("bu-lab-")
    assert first != second
