#!/usr/bin/env python3
"""Reduce iperf3 JSON to bounded Knife15 market-continuity evidence."""

from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from pathlib import Path
from typing import Any


def object_value(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def number_value(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def nonnegative_integer(value: Any, label: str) -> int:
    number = number_value(value, label)
    if number < 0 or not number.is_integer():
        raise ValueError(f"{label} must be a non-negative integer")
    return int(number)


def validate_role(
    value: dict[str, Any], *, protocol: str, reverse: bool, label: str
) -> None:
    start = object_value(value.get("start"), f"{label}.start")
    test_start = object_value(start.get("test_start"), f"{label}.start.test_start")
    if test_start.get("protocol") != protocol:
        raise ValueError(f"{label} protocol does not match {protocol}")
    observed_reverse = test_start.get("reverse")
    if isinstance(observed_reverse, bool):
        observed_reverse = int(observed_reverse)
    if observed_reverse != int(reverse):
        raise ValueError(f"{label} reverse flag does not match {int(reverse)}")
    duration = number_value(test_start.get("duration"), f"{label} duration")
    if duration <= 0:
        raise ValueError(f"{label} duration must be positive")
    object_value(value.get("end"), f"{label}.end")


def validated_result(
    value: Any, *, protocol: str, reverse: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = object_value(value, "iperf result")
    if root.get("error", "") != "":
        raise ValueError("iperf result contains an error")
    server = object_value(root.get("server_output_json"), "server_output_json")
    validate_role(root, protocol=protocol, reverse=reverse, label="client")
    validate_role(server, protocol=protocol, reverse=reverse, label="server")
    return root, server


def interval_evidence(value: dict[str, Any], *, label: str) -> dict[str, Any]:
    intervals = value.get("intervals")
    if not isinstance(intervals, list) or not intervals:
        raise ValueError(f"{label} intervals must be a non-empty array")
    test_start = object_value(
        object_value(value.get("start"), f"{label}.start").get("test_start"),
        f"{label}.start.test_start",
    )
    duration = number_value(test_start.get("duration"), f"{label} duration")

    zero_windows: list[list[float]] = []
    max_zero_run = 0
    zero_run = 0
    complete_intervals = 0
    for index, interval in enumerate(intervals):
        sample = object_value(
            object_value(interval, f"{label} interval {index}").get("sum"),
            f"{label} interval {index}.sum",
        )
        start = number_value(sample.get("start"), f"{label} interval {index} start")
        end = number_value(sample.get("end"), f"{label} interval {index} end")
        bits_per_second = number_value(
            sample.get("bits_per_second"), f"{label} interval {index} rate"
        )
        if end < start or bits_per_second < 0:
            raise ValueError(f"{label} interval {index} is invalid")
        span = end - start
        if not 0.95 <= span <= 1.25:
            proven_partial_tail = (
                index == len(intervals) - 1
                and 0 <= span < 0.95
                and start >= duration - 0.5
                and end >= duration
                and end <= duration + 0.5
            )
            if proven_partial_tail:
                continue
            raise ValueError(f"{label} interval {index} is not a complete interval")
        complete_intervals += 1
        if bits_per_second == 0:
            zero_windows.append([start, end])
            zero_run += 1
            max_zero_run = max(max_zero_run, zero_run)
        else:
            zero_run = 0
    if complete_intervals == 0:
        raise ValueError(f"{label} has no complete intervals")
    return {
        "complete_intervals": complete_intervals,
        "zero_intervals": len(zero_windows),
        "max_consecutive_zero_intervals": max_zero_run,
        "zero_windows": zero_windows,
    }


def summary_number(
    value: dict[str, Any], name: str, field: str, *, label: str
) -> float:
    end = object_value(value.get("end"), f"{label}.end")
    summary = object_value(end.get(name), f"{label}.end.{name}")
    return number_value(summary.get(field), f"{label}.end.{name}.{field}")


def summarize_tcp(value: Any, *, reverse: bool) -> dict[str, Any]:
    root, server = validated_result(value, protocol="TCP", reverse=reverse)
    sender = server if reverse else root
    receiver = root if reverse else server
    sender_intervals = interval_evidence(sender, label="sender")
    receiver_intervals = interval_evidence(receiver, label="receiver")
    sent_bytes = nonnegative_integer(
        summary_number(root, "sum_sent", "bytes", label="client"), "TCP sent bytes"
    )
    received_bytes = nonnegative_integer(
        summary_number(root, "sum_received", "bytes", label="client"),
        "TCP received bytes",
    )
    receiver_bits_per_second = summary_number(
        receiver, "sum_received", "bits_per_second", label="receiver"
    )
    if receiver_bits_per_second < 0:
        raise ValueError("TCP byte and rate summaries must be non-negative")

    return {
        "schema": "knife15-market-iperf-summary-v1",
        "protocol": "TCP",
        "reverse": int(reverse),
        "sent_bytes": sent_bytes,
        "received_bytes": received_bytes,
        "receiver_bits_per_second": receiver_bits_per_second,
        "sender_receiver_gap_bytes": abs(sent_bytes - received_bytes),
        "complete_sender_intervals": sender_intervals["complete_intervals"],
        "sender_zero_intervals": sender_intervals["zero_intervals"],
        "complete_receiver_intervals": receiver_intervals["complete_intervals"],
        "receiver_zero_intervals": receiver_intervals["zero_intervals"],
        "max_consecutive_receiver_zero_intervals": receiver_intervals[
            "max_consecutive_zero_intervals"
        ],
        "receiver_zero_windows": receiver_intervals["zero_windows"],
    }


def summarize_udp(value: Any, *, reverse: bool) -> dict[str, Any]:
    root, server = validated_result(value, protocol="UDP", reverse=reverse)
    sender = server if reverse else root
    receiver = root if reverse else server
    sender_intervals = interval_evidence(sender, label="sender")
    receiver_intervals = interval_evidence(receiver, label="receiver")
    receiver_end = object_value(receiver.get("end"), "receiver.end")
    metrics = receiver_end.get("sum", receiver_end.get("sum_received"))
    if not isinstance(metrics, dict):
        raise ValueError("UDP receiver summary is missing")
    lost_percent = number_value(metrics.get("lost_percent"), "UDP loss percent")
    if not 0 <= lost_percent <= 100:
        raise ValueError("UDP loss percent must be between 0 and 100")
    lost_packets = nonnegative_integer(metrics.get("lost_packets"), "UDP lost packets")
    packets = nonnegative_integer(metrics.get("packets"), "UDP packets")
    out_of_order = nonnegative_integer(
        metrics.get("out_of_order", 0), "UDP out of order"
    )
    jitter_ms = number_value(metrics.get("jitter_ms"), "UDP jitter")
    receiver_bits_per_second = number_value(
        metrics.get("bits_per_second"), "UDP receiver rate"
    )
    if lost_packets > packets or jitter_ms < 0 or receiver_bits_per_second < 0:
        raise ValueError("UDP packet or jitter summary is invalid")
    return {
        "schema": "knife15-market-iperf-summary-v1",
        "protocol": "UDP",
        "reverse": int(reverse),
        "lost_percent": lost_percent,
        "lost_packets": lost_packets,
        "packets": packets,
        "received_packets": packets - lost_packets,
        "out_of_order": out_of_order,
        "jitter_ms": jitter_ms,
        "receiver_bits_per_second": receiver_bits_per_second,
        "complete_sender_intervals": sender_intervals["complete_intervals"],
        "sender_zero_intervals": sender_intervals["zero_intervals"],
        "complete_receiver_intervals": receiver_intervals["complete_intervals"],
        "receiver_zero_intervals": receiver_intervals["zero_intervals"],
        "max_consecutive_receiver_zero_intervals": receiver_intervals[
            "max_consecutive_zero_intervals"
        ],
        "receiver_zero_windows": receiver_intervals["zero_windows"],
    }


def fixture_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "knife15-market" / name


def self_test() -> None:
    with fixture_path("tcp-two-zero-runs.json").open(encoding="utf-8") as handle:
        value: Any = json.load(handle)
    summary = summarize_tcp(value, reverse=False)
    assert summary["receiver_zero_intervals"] == 3
    assert summary["max_consecutive_receiver_zero_intervals"] == 2
    assert summary["receiver_bits_per_second"] == 500
    assert summary["receiver_zero_windows"] == [
        [2.0, 3.0],
        [3.0, 4.0],
        [7.0, 8.0],
    ]
    with fixture_path("tcp-partial-tail.json").open(encoding="utf-8") as handle:
        partial_value: Any = json.load(handle)
    partial = summarize_tcp(partial_value, reverse=True)
    assert partial["complete_receiver_intervals"] == 2
    assert partial["receiver_zero_intervals"] == 0
    with fixture_path("udp-complete.json").open(encoding="utf-8") as handle:
        udp_value: Any = json.load(handle)
    udp = summarize_udp(udp_value, reverse=True)
    assert udp["lost_percent"] == 1.25
    assert udp["receiver_bits_per_second"] == 7900
    assert udp["out_of_order"] == 0
    malformed_udp = copy.deepcopy(udp_value)
    malformed_udp["end"]["sum"]["lost_packets"] = "1"
    try:
        summarize_udp(malformed_udp, reverse=True)
    except ValueError:
        pass
    else:
        raise AssertionError("numeric-string UDP evidence was accepted")
    malformed_tcp = copy.deepcopy(value)
    malformed_tcp["end"]["sum_sent"]["bytes"] = 1.5
    try:
        summarize_tcp(malformed_tcp, reverse=False)
    except ValueError:
        pass
    else:
        raise AssertionError("fractional TCP bytes were accepted")
    print("knife15 market iperf summary self-test passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(
        description="Reduce iperf3 JSON to Knife15 continuity evidence"
    )
    parser.add_argument("protocol", choices=("tcp", "udp"))
    parser.add_argument("--reverse", choices=("0", "1"), required=True)
    parser.add_argument("result", help="iperf3 JSON path, or - for stdin")
    args = parser.parse_args()
    if args.result == "-":
        value: Any = json.load(sys.stdin)
    else:
        with Path(args.result).open(encoding="utf-8") as handle:
            value = json.load(handle)
    reverse = args.reverse == "1"
    summary = (
        summarize_tcp(value, reverse=reverse)
        if args.protocol == "tcp"
        else summarize_udp(value, reverse=reverse)
    )
    json.dump(summary, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, json.JSONDecodeError, KeyError, OSError, TypeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
