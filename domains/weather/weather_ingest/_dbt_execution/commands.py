"""dbt preflight and execution command construction for Weather."""

from __future__ import annotations

import json
import shlex

from .contracts import INDIRECT_SELECTION, DbtAttemptPaths, command_name


def _selection_args(selection: str) -> list[str]:
    if selection.startswith("tag:"):
        return ["--select", selection]
    return ["--selector", selection]


def resource_type(dbt_command: str) -> str:
    return {
        "source freshness": "source",
        "seed": "seed",
        "run": "model",
        "test": "test",
        "build": "model",
        "snapshot": "snapshot",
    }[command_name(dbt_command)]


def _runtime_args(
    *,
    target: str,
    target_path: str,
    log_path: str,
    variables: str | None,
    include_target_path: bool = True,
) -> list[str]:
    args = ["--target", target, "--no-use-colors"]
    if variables is not None:
        args.extend(["--vars", variables])
    if include_target_path:
        args.extend(["--target-path", target_path])
    args.extend(["--log-path", log_path])
    return args


def phase_commands(
    *,
    executable: str,
    dbt_command: str,
    selection: str | None,
    target: str,
    paths: DbtAttemptPaths,
    variables: str | None,
    fresh_parse: bool,
) -> list[tuple[str, list[str]]]:
    preflight_args = _runtime_args(
        target=target,
        target_path=paths.preflight_target_path,
        log_path=paths.preflight_log_path,
        variables=variables,
    )
    execution_args = _runtime_args(
        target=target,
        target_path=paths.execution_target_path,
        log_path=paths.execution_log_path,
        variables=variables,
        include_target_path=command_name(dbt_command) != "deps",
    )
    commands: list[tuple[str, list[str]]] = []
    if fresh_parse:
        commands.append(
            ("parse", [executable, "parse", "--no-partial-parse", *preflight_args])
        )
    if selection is not None:
        selection_args = _selection_args(selection)
        commands.append(
            (
                "ls",
                [
                    executable,
                    "ls",
                    "--resource-type",
                    resource_type(dbt_command),
                    *selection_args,
                    "--output",
                    "json",
                    "--output-keys",
                    "unique_id",
                    "resource_type",
                    INDIRECT_SELECTION,
                    *preflight_args,
                ],
            )
        )
        actual_args = [
            executable,
            *shlex.split(dbt_command),
            *selection_args,
            INDIRECT_SELECTION,
            *execution_args,
        ]
    else:
        actual_args = [executable, *shlex.split(dbt_command), *execution_args]
    commands.append(("command", actual_args))
    return commands


def selected_unique_ids(stdout: str) -> tuple[str, ...]:
    selected: list[str] = []
    for line in stdout.splitlines():
        try:
            node = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(node, dict) and node.get("unique_id"):
            selected.append(str(node["unique_id"]))
    return tuple(dict.fromkeys(selected))
