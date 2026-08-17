#!/usr/bin/env python3
"""Reduce sealed six-hour Knife15 epochs to Tier-B continuity evidence."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import ipaddress
import json
import math
import re
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any


COLLECTION_SCHEMA = "knife15-m2-frequency-collection-v1"
EPOCH_SCHEMA = "knife15-m2-frequency-epoch-v1"
RESULT_SCHEMA = "knife15-m2-frequency-result-v1"
SCENARIO_SCHEMA = "knife15-m2-frequency-scenario-v1"
GENERATED_SCENARIO_SCHEMA = "knife15-m2-frequency-generated-scenario-v1"
EPOCH_SECONDS = 6 * 60 * 60
EPOCH_NANOSECONDS = EPOCH_SECONDS * 1_000_000_000
SIX_HOURS_NS = EPOCH_NANOSECONDS
TWENTY_FOUR_HOURS_NS = 4 * EPOCH_NANOSECONDS
CLOCK_SKEW_TOLERANCE_NS = 1_000_000_000
INTERVAL_CONTIGUITY_TOLERANCE_NS = 10_000_000
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_EPOCHS = 12
MAX_TCP_RESULTS_PER_EPOCH = 4096
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
UTC_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z")
IDENTITY_FIELDS = {
    "candidate_id",
    "source_commit",
    "binary_sha256",
    "runner_sha256",
    "resource_profile_sha256",
    "workload_contract_sha256",
    "server_binary_sha256",
    "server_config_sha256",
    "observer_sha256",
    "exit_ipv4",
    "target_ipv4",
    "tuic_port",
    "target_iperf_port",
}
EPOCH_FIELDS = {
    "schema",
    "epoch_id",
    "evidence_segment_id",
    "evidence_segment_index",
    "lifetime_id",
    "lifetime_index",
    "gap_before",
    "started_utc",
    "ended_utc",
    "started_monotonic_ns",
    "ended_monotonic_ns",
    "valid",
    "identity",
    "tcp_results",
}
TCP_RESULT_FIELDS = {
    "result_id",
    "path",
    "reverse",
    "started_utc",
    "started_monotonic_ns",
}
GAP_FIELDS = {
    "reason",
    "started_utc",
    "ended_utc",
    "started_monotonic_ns",
    "ended_monotonic_ns",
}
ALLOWED_GAP_REASONS = {"evidence-unavailable", "invalid-infrastructure"}


class FrequencyError(ValueError):
    """Raised when frequency evidence is not admissible."""


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FrequencyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    if path.stat().st_size > MAX_JSON_BYTES:
        raise FrequencyError(f"JSON evidence exceeds {MAX_JSON_BYTES} bytes: {path.name}")
    with path.open(encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=unique_object)


def exact_object(value: Any, fields: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FrequencyError(f"{label} must be an object")
    missing = sorted(fields - value.keys())
    unknown = sorted(value.keys() - fields)
    if missing or unknown:
        raise FrequencyError(f"{label} fields mismatch: missing={missing} unknown={unknown}")
    return value


def exact_integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FrequencyError(f"{label} must be an integer >= {minimum}")
    return value


def token(value: Any, label: str) -> str:
    if not isinstance(value, str) or TOKEN_RE.fullmatch(value) is None:
        raise FrequencyError(f"{label} is invalid")
    return value


def utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or UTC_RE.fullmatch(value) is None:
        raise FrequencyError(f"{label} must be a canonical UTC timestamp")
    return datetime.fromisoformat(value[:-1] + "+00:00")


def utc_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def load_market_module() -> Any:
    path = Path(__file__).resolve().with_name("knife15-market-iperf-summary.py")
    spec = importlib.util.spec_from_file_location("knife15_market_iperf_summary", path)
    if spec is None or spec.loader is None:
        raise FrequencyError("cannot load reviewed iperf summary module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MARKET = load_market_module()


def validate_identity(value: Any) -> dict[str, Any]:
    identity = exact_object(value, IDENTITY_FIELDS, "identity")
    token(identity["candidate_id"], "identity.candidate_id")
    if not isinstance(identity["source_commit"], str) or COMMIT_RE.fullmatch(
        identity["source_commit"]
    ) is None:
        raise FrequencyError("identity.source_commit is invalid")
    for field in sorted(name for name in IDENTITY_FIELDS if name.endswith("sha256")):
        value = identity[field]
        if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
            raise FrequencyError(f"identity.{field} is invalid")
    for field in ("exit_ipv4", "target_ipv4"):
        if not isinstance(identity[field], str):
            raise FrequencyError(f"identity.{field} is invalid")
        try:
            address = ipaddress.ip_address(identity[field])
        except ValueError as error:
            raise FrequencyError(f"identity.{field} is invalid") from error
        if address.version != 4 or str(address) != identity[field]:
            raise FrequencyError(f"identity.{field} is not canonical IPv4")
    for field in ("tuic_port", "target_iperf_port"):
        port = exact_integer(identity[field], f"identity.{field}", minimum=1)
        if port > 65535:
            raise FrequencyError(f"identity.{field} is invalid")
    return identity


def safe_result_path(root: Path, value: Any) -> Path:
    if not isinstance(value, str):
        raise FrequencyError("TCP result path must be a string")
    relative = PurePosixPath(value)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise FrequencyError("TCP result path must stay below the evidence root")
    canonical_root = root.resolve(strict=True)
    path = canonical_root.joinpath(*relative.parts)
    if path.is_symlink() or not path.is_file():
        raise FrequencyError(f"TCP result is missing or not a regular file: {value}")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(canonical_root) or resolved != path:
        raise FrequencyError("TCP result path escapes through a symbolic link")
    if path.stat().st_size > MAX_JSON_BYTES:
        raise FrequencyError(f"TCP result exceeds 8 MiB: {value}")
    return resolved


def identity_digest(identity: dict[str, Any]) -> str:
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def positive_seconds_ns(value: Any, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise FrequencyError(f"{label} is invalid")
    result = round(float(value) * 1_000_000_000)
    if result <= 0:
        raise FrequencyError(f"{label} is invalid")
    return result


def test_duration_ns(raw_iperf: Any, result_id: str) -> int:
    if not isinstance(raw_iperf, dict):
        raise FrequencyError(f"TCP result {result_id} is not an object")
    start = raw_iperf.get("start")
    test_start = start.get("test_start") if isinstance(start, dict) else None
    duration = test_start.get("duration") if isinstance(test_start, dict) else None
    client_duration_ns = positive_seconds_ns(
        duration, f"TCP result {result_id} client duration"
    )
    server = raw_iperf.get("server_output_json")
    server_start = server.get("start") if isinstance(server, dict) else None
    server_test_start = (
        server_start.get("test_start") if isinstance(server_start, dict) else None
    )
    server_duration = (
        server_test_start.get("duration")
        if isinstance(server_test_start, dict)
        else None
    )
    server_duration_ns = positive_seconds_ns(
        server_duration, f"TCP result {result_id} server duration"
    )
    if server_duration_ns != client_duration_ns:
        raise FrequencyError(f"TCP result {result_id} client/server durations disagree")
    return client_duration_ns


def validate_interval_sequences(raw_iperf: Any, result_id: str) -> None:
    if not isinstance(raw_iperf, dict):
        raise FrequencyError(f"TCP result {result_id} is not an object")
    endpoints = (("client", raw_iperf), ("server", raw_iperf.get("server_output_json")))
    for label, endpoint in endpoints:
        if not isinstance(endpoint, dict):
            raise FrequencyError(f"TCP result {result_id} {label} evidence is missing")
        start = endpoint.get("start")
        test_start = start.get("test_start") if isinstance(start, dict) else None
        duration = test_start.get("duration") if isinstance(test_start, dict) else None
        duration_ns = positive_seconds_ns(
            duration, f"TCP result {result_id} {label} duration"
        )
        intervals = endpoint.get("intervals")
        if not isinstance(intervals, list) or not intervals:
            raise FrequencyError(f"TCP result {result_id} {label} intervals are missing")
        previous_end_ns: int | None = None
        for index, interval in enumerate(intervals):
            sample = interval.get("sum") if isinstance(interval, dict) else None
            if not isinstance(sample, dict):
                raise FrequencyError(
                    f"TCP result {result_id} {label} interval {index} is malformed"
                )
            start_value = sample.get("start")
            end_value = sample.get("end")
            if (
                isinstance(start_value, bool)
                or not isinstance(start_value, (int, float))
                or not math.isfinite(float(start_value))
                or isinstance(end_value, bool)
                or not isinstance(end_value, (int, float))
                or not math.isfinite(float(end_value))
            ):
                raise FrequencyError(
                    f"TCP result {result_id} {label} interval {index} time is invalid"
                )
            start_ns = round(float(start_value) * 1_000_000_000)
            end_ns = round(float(end_value) * 1_000_000_000)
            span_ns = end_ns - start_ns
            complete = 950_000_000 <= span_ns <= 1_250_000_000
            if not complete:
                partial_tail = (
                    index == len(intervals) - 1
                    and 0 <= span_ns < 950_000_000
                    and start_ns >= duration_ns - 500_000_000
                    and end_ns >= duration_ns
                    and end_ns <= duration_ns + 500_000_000
                )
                if partial_tail:
                    continue
                # The reviewed market reducer will report the precise span error.
                continue
            if previous_end_ns is None:
                if abs(start_ns) > INTERVAL_CONTIGUITY_TOLERANCE_NS:
                    raise FrequencyError(
                        f"TCP result {result_id} {label} interval sequence has a gap"
                    )
            elif abs(start_ns - previous_end_ns) > INTERVAL_CONTIGUITY_TOLERANCE_NS:
                raise FrequencyError(
                    f"TCP result {result_id} {label} interval sequence has a gap"
                )
            previous_end_ns = end_ns
        if previous_end_ns is None or previous_end_ns < duration_ns - 500_000_000:
            raise FrequencyError(
                f"TCP result {result_id} {label} interval sequence does not cover duration"
            )


def receiver_zero_episodes(summary: dict[str, Any]) -> list[dict[str, int]]:
    episodes: list[dict[str, int]] = []
    for raw_window in summary["receiver_zero_windows"]:
        if not isinstance(raw_window, list) or len(raw_window) != 2:
            raise FrequencyError("receiver zero window is malformed")
        start_ns = round(float(raw_window[0]) * 1_000_000_000)
        end_ns = round(float(raw_window[1]) * 1_000_000_000)
        if end_ns <= start_ns:
            raise FrequencyError("receiver zero window is malformed")
        if (
            episodes
            and abs(episodes[-1]["end_ns"] - start_ns)
            <= INTERVAL_CONTIGUITY_TOLERANCE_NS
        ):
            episodes[-1]["end_ns"] = end_ns
            episodes[-1]["intervals"] += 1
        else:
            episodes.append(
                {"start_ns": start_ns, "end_ns": end_ns, "intervals": 1}
            )
    return episodes


def rolling_maximum(
    events: list[dict[str, Any]], *, clock: str, window_ns: int, field: str
) -> int:
    maximum = 0
    for segment_events in _events_by_segment(events).values():
        ordered = sorted(segment_events, key=lambda event: event[clock])
        active: deque[dict[str, Any]] = deque()
        total = 0
        for event in ordered:
            boundary = event[clock] - window_ns
            while active and active[0][clock] <= boundary:
                total -= active.popleft()[field]
            active.append(event)
            total += event[field]
            maximum = max(maximum, total)
    return maximum


def _events_by_segment(
    events: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        result[event["evidence_segment_id"]].append(event)
    return result


def checked_rolling_maximum(
    events: list[dict[str, Any]], *, window_ns: int, field: str
) -> int:
    utc_maximum = rolling_maximum(
        events, clock="utc_ns", window_ns=window_ns, field=field
    )
    monotonic_maximum = rolling_maximum(
        events, clock="monotonic_ns", window_ns=window_ns, field=field
    )
    if utc_maximum != monotonic_maximum:
        raise FrequencyError("UTC and monotonic rolling-window results disagree")
    return utc_maximum


def summarize_collection(value: Any, *, evidence_root: Path) -> dict[str, Any]:
    collection = exact_object(value, {"schema", "epochs"}, "collection")
    if collection["schema"] != COLLECTION_SCHEMA:
        raise FrequencyError("collection schema is invalid")
    epochs = collection["epochs"]
    if not isinstance(epochs, list) or not epochs:
        raise FrequencyError("collection.epochs must be a non-empty array")
    if len(epochs) > MAX_EPOCHS:
        raise FrequencyError(f"collection exceeds the {MAX_EPOCHS}-epoch contract")

    expected_identity: dict[str, Any] | None = None
    events: list[dict[str, Any]] = []
    max_consecutive = 0
    max_contiguous_lifetime = 0
    current_contiguous_lifetime = 0
    epoch_ids: set[str] = set()
    result_ids: set[str] = set()
    seen_evidence_ids: set[str] = set()
    seen_lifetime_ids: set[str] = set()
    previous_epoch: dict[str, Any] | None = None

    for epoch_index, raw_epoch in enumerate(epochs):
        epoch = exact_object(raw_epoch, EPOCH_FIELDS, f"epoch {epoch_index}")
        if epoch["schema"] != EPOCH_SCHEMA:
            raise FrequencyError(f"epoch {epoch_index} schema is invalid")
        epoch_id = token(epoch["epoch_id"], f"epoch {epoch_index}.epoch_id")
        if epoch_id in epoch_ids:
            raise FrequencyError(f"duplicate epoch id: {epoch_id}")
        epoch_ids.add(epoch_id)
        if epoch["valid"] is not True:
            raise FrequencyError(f"epoch {epoch_id} is not valid")

        evidence_id = token(
            epoch["evidence_segment_id"], f"epoch {epoch_id} evidence segment"
        )
        evidence_index = exact_integer(
            epoch["evidence_segment_index"], f"epoch {epoch_id} evidence index"
        )
        lifetime_id = token(epoch["lifetime_id"], f"epoch {epoch_id} lifetime")
        lifetime_index = exact_integer(
            epoch["lifetime_index"], f"epoch {epoch_id} lifetime index"
        )
        started_utc = utc(epoch["started_utc"], f"epoch {epoch_id} start")
        ended_utc = utc(epoch["ended_utc"], f"epoch {epoch_id} end")
        started_utc_ns = utc_ns(started_utc)
        ended_utc_ns = utc_ns(ended_utc)
        started_mono = exact_integer(
            epoch["started_monotonic_ns"], f"epoch {epoch_id} monotonic start"
        )
        ended_mono = exact_integer(
            epoch["ended_monotonic_ns"], f"epoch {epoch_id} monotonic end"
        )
        if ended_utc_ns - started_utc_ns != EPOCH_NANOSECONDS:
            raise FrequencyError(f"epoch {epoch_id} is not exactly six UTC hours")
        if ended_mono - started_mono != EPOCH_NANOSECONDS:
            raise FrequencyError(f"epoch {epoch_id} is not exactly six monotonic hours")

        if previous_epoch is None:
            if evidence_index != 0 or lifetime_index != 0 or epoch["gap_before"] is not None:
                raise FrequencyError("first epoch must start evidence and lifetime index zero")
            current_contiguous_lifetime = 1
            seen_evidence_ids.add(evidence_id)
            seen_lifetime_ids.add(lifetime_id)
        else:
            if started_utc_ns < previous_epoch["ended_utc_ns"]:
                raise FrequencyError(f"epoch {epoch_id} UTC clock overlaps or reverses")
            if started_mono < previous_epoch["ended_monotonic_ns"]:
                raise FrequencyError(f"epoch {epoch_id} monotonic clock overlaps or reverses")
            same_evidence = evidence_id == previous_epoch["evidence_id"]
            utc_contiguous = started_utc_ns == previous_epoch["ended_utc_ns"]
            mono_contiguous = started_mono == previous_epoch["ended_monotonic_ns"]
            if same_evidence:
                if epoch["gap_before"] is not None:
                    raise FrequencyError(
                        f"epoch {epoch_id} cannot put a gap inside one evidence segment"
                    )
                if evidence_index != previous_epoch["evidence_index"] + 1:
                    raise FrequencyError(f"epoch {epoch_id} is missing an evidence-segment epoch")
                if not utc_contiguous or not mono_contiguous:
                    raise FrequencyError(f"epoch {epoch_id} leaves an unsealed evidence gap")
            else:
                if evidence_index != 0:
                    raise FrequencyError(
                        f"epoch {epoch_id} new evidence segment must start at zero"
                    )
                if evidence_id in seen_evidence_ids:
                    raise FrequencyError(f"epoch {epoch_id} reuses a closed evidence segment")
                seen_evidence_ids.add(evidence_id)
                gap = exact_object(epoch["gap_before"], GAP_FIELDS, f"epoch {epoch_id} gap")
                gap_reason = token(gap["reason"], f"epoch {epoch_id} gap reason")
                if gap_reason not in ALLOWED_GAP_REASONS:
                    raise FrequencyError(f"epoch {epoch_id} gap reason is not admitted")
                gap_started_utc = utc(gap["started_utc"], f"epoch {epoch_id} gap start")
                gap_ended_utc = utc(gap["ended_utc"], f"epoch {epoch_id} gap end")
                gap_started_mono = exact_integer(
                    gap["started_monotonic_ns"], f"epoch {epoch_id} gap monotonic start"
                )
                gap_ended_mono = exact_integer(
                    gap["ended_monotonic_ns"], f"epoch {epoch_id} gap monotonic end"
                )
                if (
                    utc_ns(gap_started_utc) != previous_epoch["ended_utc_ns"]
                    or utc_ns(gap_ended_utc) != started_utc_ns
                    or gap_started_mono != previous_epoch["ended_monotonic_ns"]
                    or gap_ended_mono != started_mono
                    or gap_ended_utc <= gap_started_utc
                    or gap_ended_mono <= gap_started_mono
                ):
                    raise FrequencyError(
                        f"epoch {epoch_id} gap does not exactly cover the unknown period"
                    )
                utc_gap_ns = utc_ns(gap_ended_utc) - utc_ns(gap_started_utc)
                monotonic_gap_ns = gap_ended_mono - gap_started_mono
                if abs(utc_gap_ns - monotonic_gap_ns) > CLOCK_SKEW_TOLERANCE_NS:
                    raise FrequencyError(
                        f"epoch {epoch_id} gap UTC/monotonic clocks disagree"
                    )

            same_lifetime = lifetime_id == previous_epoch["lifetime_id"]
            if same_lifetime:
                if lifetime_index != previous_epoch["lifetime_index"] + 1:
                    raise FrequencyError(f"epoch {epoch_id} is missing a lifetime index")
            else:
                if lifetime_index != 0:
                    raise FrequencyError(f"epoch {epoch_id} new lifetime must start at zero")
                if lifetime_id in seen_lifetime_ids:
                    raise FrequencyError(f"epoch {epoch_id} reuses a closed lifetime")
                seen_lifetime_ids.add(lifetime_id)
            if same_lifetime and utc_contiguous and mono_contiguous:
                current_contiguous_lifetime += 1
            else:
                current_contiguous_lifetime = 1
        max_contiguous_lifetime = max(
            max_contiguous_lifetime, current_contiguous_lifetime
        )

        identity = validate_identity(epoch["identity"])
        if expected_identity is None:
            expected_identity = identity
        elif identity != expected_identity:
            raise FrequencyError(f"epoch {epoch_id} identity does not match")

        results = epoch["tcp_results"]
        if not isinstance(results, list) or not results:
            raise FrequencyError(f"epoch {epoch_id} has no TCP results")
        if len(results) > MAX_TCP_RESULTS_PER_EPOCH:
            raise FrequencyError(
                f"epoch {epoch_id} exceeds {MAX_TCP_RESULTS_PER_EPOCH} TCP results"
            )
        previous_result_end_utc_ns: int | None = None
        previous_result_end_mono: int | None = None
        for result_index, raw_result in enumerate(results):
            result = exact_object(
                raw_result, TCP_RESULT_FIELDS, f"epoch {epoch_id} TCP result {result_index}"
            )
            result_id = token(result["result_id"], "TCP result id")
            if result_id in result_ids:
                raise FrequencyError(f"duplicate TCP result id: {result_id}")
            result_ids.add(result_id)
            reverse = exact_integer(result["reverse"], f"TCP result {result_id} reverse")
            if reverse not in (0, 1):
                raise FrequencyError(f"TCP result {result_id} reverse must be 0 or 1")
            result_utc = utc(result["started_utc"], f"TCP result {result_id} start")
            result_utc_ns = utc_ns(result_utc)
            result_mono = exact_integer(
                result["started_monotonic_ns"], f"TCP result {result_id} monotonic start"
            )
            utc_offset = result_utc_ns - started_utc_ns
            mono_offset = result_mono - started_mono
            if not 0 <= utc_offset < EPOCH_NANOSECONDS:
                raise FrequencyError(f"TCP result {result_id} is outside its epoch")
            if not 0 <= mono_offset < EPOCH_NANOSECONDS:
                raise FrequencyError(f"TCP result {result_id} monotonic time is outside its epoch")
            if abs(utc_offset - mono_offset) > CLOCK_SKEW_TOLERANCE_NS:
                raise FrequencyError(f"TCP result {result_id} UTC/monotonic clocks disagree")

            raw_iperf = read_json(safe_result_path(evidence_root, result["path"]))
            validate_interval_sequences(raw_iperf, result_id)
            summary = MARKET.summarize_tcp(raw_iperf, reverse=bool(reverse))
            duration_ns = test_duration_ns(raw_iperf, result_id)
            result_end_utc_ns = result_utc_ns + duration_ns
            result_end_mono = result_mono + duration_ns
            if result_end_utc_ns > ended_utc_ns or result_end_mono > ended_mono:
                raise FrequencyError(f"TCP result {result_id} extends beyond its epoch")
            if (
                previous_result_end_utc_ns is not None
                and previous_result_end_mono is not None
                and (
                    result_utc_ns < previous_result_end_utc_ns
                    or result_mono < previous_result_end_mono
                )
            ):
                raise FrequencyError(f"TCP result {result_id} overlaps or reverses")
            previous_result_end_utc_ns = result_end_utc_ns
            previous_result_end_mono = result_end_mono

            max_consecutive = max(
                max_consecutive, summary["max_consecutive_receiver_zero_intervals"]
            )
            for episode in receiver_zero_episodes(summary):
                events.append(
                    {
                        "evidence_segment_id": evidence_id,
                        "utc_ns": result_utc_ns + episode["start_ns"],
                        "monotonic_ns": result_mono + episode["start_ns"],
                        "episodes": 1,
                        "intervals": episode["intervals"],
                    }
                )

        previous_epoch = {
            "evidence_id": evidence_id,
            "evidence_index": evidence_index,
            "lifetime_id": lifetime_id,
            "lifetime_index": lifetime_index,
            "ended_utc_ns": ended_utc_ns,
            "ended_monotonic_ns": ended_mono,
        }

    assert expected_identity is not None
    for segment_events in _events_by_segment(events).values():
        if [event["utc_ns"] for event in segment_events] != sorted(
            event["utc_ns"] for event in segment_events
        ):
            raise FrequencyError("receiver-zero UTC events reverse within an evidence segment")
        if [event["monotonic_ns"] for event in segment_events] != sorted(
            event["monotonic_ns"] for event in segment_events
        ):
            raise FrequencyError(
                "receiver-zero monotonic events reverse within an evidence segment"
            )

    receiver_zero_intervals = sum(event["intervals"] for event in events)
    receiver_zero_episodes_count = len(events)
    maximum_6h_episodes = checked_rolling_maximum(
        events, window_ns=SIX_HOURS_NS, field="episodes"
    )
    maximum_24h_episodes = checked_rolling_maximum(
        events, window_ns=TWENTY_FOUR_HOURS_NS, field="episodes"
    )
    maximum_24h_intervals = checked_rolling_maximum(
        events, window_ns=TWENTY_FOUR_HOURS_NS, field="intervals"
    )
    violations: list[str] = []
    if max_consecutive > 1:
        violations.append("max_consecutive_receiver_zero_intervals")
    if maximum_6h_episodes > 1:
        violations.append("receiver_zero_episodes_in_any_rolling_6h")
    if maximum_24h_episodes > 3:
        violations.append("receiver_zero_episodes_in_any_rolling_24h")
    if maximum_24h_intervals > 3:
        violations.append("receiver_zero_intervals_in_any_rolling_24h")
    summary = {
        "schema": RESULT_SCHEMA,
        "status": "PASS" if not violations else "FAIL",
        "valid_epochs": len(epochs),
        "valid_hours": len(epochs) * 6,
        "identity_sha256": identity_digest(expected_identity),
        "receiver_zero_episodes": receiver_zero_episodes_count,
        "receiver_zero_intervals": receiver_zero_intervals,
        "max_consecutive_receiver_zero_intervals": max_consecutive,
        "max_receiver_zero_episodes_rolling_6h": maximum_6h_episodes,
        "max_receiver_zero_episodes_rolling_24h": maximum_24h_episodes,
        "max_receiver_zero_intervals_rolling_24h": maximum_24h_intervals,
        "max_contiguous_valid_epochs_same_lifetime": max_contiguous_lifetime,
        "violations": violations,
    }
    return summary


def fixture_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "knife15-m2-frequency" / name


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def shift_generated_epoch(epoch: dict[str, Any], seconds: int) -> None:
    epoch["started_utc"] = format_utc(
        utc(epoch["started_utc"], "generated gap epoch start")
        + timedelta(seconds=seconds)
    )
    epoch["ended_utc"] = format_utc(
        utc(epoch["ended_utc"], "generated gap epoch end")
        + timedelta(seconds=seconds)
    )
    shift_ns = seconds * 1_000_000_000
    epoch["started_monotonic_ns"] += shift_ns
    epoch["ended_monotonic_ns"] += shift_ns
    for result in epoch["tcp_results"]:
        result["started_utc"] = format_utc(
            utc(result["started_utc"], "generated gap result start")
            + timedelta(seconds=seconds)
        )
        result["started_monotonic_ns"] += shift_ns


def open_generated_evidence_segment(
    epochs: list[dict[str, Any]], start_index: int, segment_id: str
) -> None:
    previous_end_utc = epochs[start_index - 1]["ended_utc"]
    previous_end_mono = epochs[start_index - 1]["ended_monotonic_ns"]
    for epoch in epochs[start_index:]:
        shift_generated_epoch(epoch, 1)
    for index in range(start_index, len(epochs)):
        epochs[index]["evidence_segment_id"] = segment_id
        epochs[index]["evidence_segment_index"] = index - start_index
    epochs[start_index]["gap_before"] = {
        "reason": "invalid-infrastructure",
        "started_utc": previous_end_utc,
        "ended_utc": epochs[start_index]["started_utc"],
        "started_monotonic_ns": previous_end_mono,
        "ended_monotonic_ns": epochs[start_index]["started_monotonic_ns"],
    }


def generated_collection(scenario: dict[str, Any]) -> dict[str, Any]:
    exact_object(
        scenario,
        {"schema", "epoch_count", "events", "mutation", "expect"},
        "generated scenario",
    )
    epoch_count = exact_integer(scenario["epoch_count"], "scenario epoch_count", minimum=1)
    if not isinstance(scenario["events"], list):
        raise FrequencyError("scenario events must be an array")
    base_scenario = read_json(fixture_path("one-isolated-pass.json"))
    base_collection = exact_object(
        base_scenario, {"schema", "expect", "collection"}, "base scenario"
    )["collection"]
    template = copy.deepcopy(base_collection["epochs"][0])
    first_utc = utc(template["started_utc"], "base epoch start")
    first_mono = exact_integer(template["started_monotonic_ns"], "base monotonic start")
    epochs: list[dict[str, Any]] = []
    for index in range(epoch_count):
        epoch = copy.deepcopy(template)
        epoch_start_utc = first_utc + timedelta(seconds=index * EPOCH_SECONDS)
        epoch_end_utc = epoch_start_utc + timedelta(seconds=EPOCH_SECONDS)
        epoch_start_mono = first_mono + index * EPOCH_NANOSECONDS
        epoch["epoch_id"] = f"epoch-{index:03d}"
        epoch["evidence_segment_index"] = index
        epoch["lifetime_index"] = index
        epoch["started_utc"] = format_utc(epoch_start_utc)
        epoch["ended_utc"] = format_utc(epoch_end_utc)
        epoch["started_monotonic_ns"] = epoch_start_mono
        epoch["ended_monotonic_ns"] = epoch_start_mono + EPOCH_NANOSECONDS
        epoch["tcp_results"] = [
            {
                "result_id": f"filler-{index:03d}",
                "path": "results/partial-final-zero.json",
                "reverse": 1,
                "started_utc": format_utc(epoch_start_utc + timedelta(seconds=60)),
                "started_monotonic_ns": epoch_start_mono + 60_000_000_000,
            }
        ]
        epochs.append(epoch)

    for event_index, raw_event in enumerate(scenario["events"]):
        event = exact_object(
            raw_event, {"offset_seconds", "path", "reverse"}, f"scenario event {event_index}"
        )
        offset = exact_integer(event["offset_seconds"], f"scenario event {event_index} offset")
        if offset >= epoch_count * EPOCH_SECONDS:
            raise FrequencyError(f"scenario event {event_index} is outside generated epochs")
        epoch_index = offset // EPOCH_SECONDS
        epoch = epochs[epoch_index]
        result_utc = first_utc + timedelta(seconds=offset)
        result_mono = first_mono + offset * 1_000_000_000
        epoch["tcp_results"].append(
            {
                "result_id": f"event-{event_index:03d}",
                "path": event["path"],
                "reverse": event["reverse"],
                "started_utc": format_utc(result_utc),
                "started_monotonic_ns": result_mono,
            }
        )
    for epoch in epochs:
        epoch["tcp_results"].sort(key=lambda result: result["started_monotonic_ns"])

    mutation = scenario["mutation"]
    if mutation == "none":
        pass
    elif mutation == "missing_epoch":
        epochs[1]["evidence_segment_index"] = 2
    elif mutation == "clock_reversal":
        epochs[1]["started_monotonic_ns"] -= 1
        epochs[1]["ended_monotonic_ns"] -= 1
    elif mutation == "identity_mismatch":
        epochs[1]["identity"]["binary_sha256"] = "9" * 64
    elif mutation == "invalid_epoch":
        epochs[1]["valid"] = False
    elif mutation == "evidence_gap":
        open_generated_evidence_segment(epochs, 3, "evidence-b")
    elif mutation == "segment_reuse":
        open_generated_evidence_segment(epochs, 1, "evidence-b")
        open_generated_evidence_segment(epochs, 2, "evidence-a")
    else:
        raise FrequencyError(f"unknown generated scenario mutation: {mutation}")
    return {"schema": COLLECTION_SCHEMA, "epochs": epochs}


def self_test() -> None:
    scenario_path = fixture_path("one-isolated-pass.json")
    scenario = exact_object(
        read_json(scenario_path), {"schema", "expect", "collection"}, "scenario"
    )
    assert scenario["schema"] == SCENARIO_SCHEMA
    result = summarize_collection(scenario["collection"], evidence_root=scenario_path.parent)
    for field, expected in scenario["expect"].items():
        assert result[field] == expected, (field, result[field], expected)
    mismatched_duration = read_json(fixture_path("results/one-zero.json"))
    mismatched_duration["server_output_json"]["start"]["test_start"]["duration"] = 3
    try:
        test_duration_ns(mismatched_duration, "duration-mismatch")
    except FrequencyError as error:
        assert "client/server durations disagree" in str(error)
    else:
        raise AssertionError("mismatched client/server durations were accepted")
    try:
        positive_seconds_ns(float("inf"), "non-finite duration")
    except FrequencyError:
        pass
    else:
        raise AssertionError("non-finite duration was accepted")
    for generated_path in sorted(scenario_path.parent.glob("*.json")):
        generated = read_json(generated_path)
        if not isinstance(generated, dict) or generated.get("schema") != GENERATED_SCENARIO_SCHEMA:
            continue
        collection = generated_collection(generated)
        expected = generated["expect"]
        if "error_contains" in expected:
            try:
                summarize_collection(collection, evidence_root=scenario_path.parent)
            except FrequencyError as error:
                assert expected["error_contains"] in str(error), (
                    generated_path.name,
                    str(error),
                    expected["error_contains"],
                )
            else:
                raise AssertionError(f"{generated_path.name} did not fail closed")
            continue
        result = summarize_collection(collection, evidence_root=scenario_path.parent)
        for field, expected_value in expected.items():
            assert result[field] == expected_value, (
                generated_path.name,
                field,
                result[field],
                expected_value,
            )
    print("knife15 M2 frequency summary self-test passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(
        description="Reduce sealed Knife15 six-hour epochs to Tier-B continuity evidence"
    )
    parser.add_argument("collection", help="frequency collection JSON path, or - for stdin")
    parser.add_argument(
        "--evidence-root",
        help="root for relative iperf result paths (required with stdin)",
    )
    args = parser.parse_args()
    if args.collection == "-":
        if args.evidence_root is None:
            raise FrequencyError("--evidence-root is required with stdin")
        raw_stdin = sys.stdin.buffer.read(MAX_JSON_BYTES + 1)
        if len(raw_stdin) > MAX_JSON_BYTES:
            raise FrequencyError(f"stdin collection exceeds {MAX_JSON_BYTES} bytes")
        value = json.loads(raw_stdin.decode(), object_pairs_hook=unique_object)
        evidence_root = Path(args.evidence_root)
    else:
        collection_path = Path(args.collection)
        value = read_json(collection_path)
        evidence_root = (
            Path(args.evidence_root) if args.evidence_root else collection_path.parent
        )
    result = summarize_collection(value, evidence_root=evidence_root)
    json.dump(result, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        AssertionError,
        FrequencyError,
        json.JSONDecodeError,
        OSError,
        TypeError,
        UnicodeError,
        ValueError,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
