#!/usr/bin/env python3
"""Validate and reduce the bounded Knife15 Tier-A strict-attempt ledger."""

from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
import re
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any


LEDGER_SCHEMA = "knife15-m2-tier-a-ledger-v1"
ATTEMPT_SCHEMA = "knife15-m2-tier-a-attempt-v1"
RESULT_SCHEMA = "knife15-m2-tier-a-ledger-result-v1"
SCENARIO_SCHEMA = "knife15-m2-ledger-scenario-v1"
STRICT_RESOURCE_SOURCE_FLOOR = "211e7c3"
REFERENCE = {
    "candidate_id": "reference-33",
    "exit_ipv4": "43.153.32.33",
    "source_commit": "cdbfe36ae72f9049e766e4c140476af146e15064",
    "mac_bundle_sha256": (
        "2c0026847884602d0f44b359b7081974fd4478413fd14cfbd631b880c3203329"
    ),
    "exit_bundle_sha256": (
        "ba37874360aebdc0bb189f45876031ad439a520baed1a999e8bda72c35724501"
    ),
    "verdict": "REJECTED_STRICT_REFERENCE",
}
LEDGER_FIELDS = {"schema", "reference", "attempts"}
ATTEMPT_FIELDS = {
    "schema",
    "sequence",
    "candidate_id",
    "resource_identity_sha256",
    "resource_profile_sha256",
    "source_commit",
    "binary_sha256",
    "runner_sha256",
    "resource_profile_helper_sha256",
    "resource_preflight_runner_sha256",
    "workload_contract_sha256",
    "server_binary_sha256",
    "server_config_sha256",
    "observer_sha256",
    "exit_ipv4",
    "tuic_port",
    "target_ipv4",
    "target_iperf_port",
    "role",
    "evidence_class",
    "reason",
    "started_utc",
    "ended_utc",
    "mac_bundle_name",
    "mac_bundle_sha256",
    "exit_bundle_name",
    "exit_bundle_sha256",
    "resource_admission_pass",
    "observer_match",
    "cleanup_pass",
    "result_integrity_pass",
    "safety_pass",
    "strict_sli_pass",
    "receiver_zero_intervals",
    "udp_max_loss_percent",
}
PASS_REASONS = {"strict_pass"}
QUALITY_REASONS = {
    "receiver_zero",
    "udp_loss",
    "safety_failure",
    "lifecycle_failure",
}
INVALID_REASONS = {
    "environment_invalid",
    "operator_error",
    "power_loss",
    "vps_outage",
    "evidence_incomplete",
}
TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
IPV4_RE = re.compile(r"(?:[0-9]{1,3}\.){3}[0-9]{1,3}\Z")
BUNDLE_RE = re.compile(r"mini_vpn_knife15_[A-Za-z0-9._-]+\.tar\.gz\Z")
RESOURCE_IDENTITY_FIELDS = (
    "provider",
    "resource_id",
    "region",
    "public_ipv4",
    "asn",
    "route_class",
    "route_contract_id",
    "provider_identity_evidence_sha256",
    "route_identity_evidence_sha256",
    "tuic_port",
    "target_ipv4",
    "target_iperf_port",
    "server_binary_sha256",
    "server_config_sha256",
    "prior_saturation_proven",
    "prior_saturation_evidence_sha256",
    "replacement_capacity_proven",
    "replacement_capacity_evidence_sha256",
)
RESOURCE_PROFILE_FIELDS = {
    "schema",
    "candidate_id",
    "provider",
    "resource_id",
    "region",
    "public_ipv4",
    "asn",
    "route_class",
    "route_contract_id",
    "provider_identity_evidence_sha256",
    "route_identity_evidence_sha256",
    "tuic_port",
    "target_ipv4",
    "target_iperf_port",
    "server_binary_sha256",
    "server_config_sha256",
    "observer_sha256",
    "source_commit",
    "client_binary_sha256",
    "workload_profile_sha256",
    "mac_interface",
    "prior_saturation_proven",
    "prior_saturation_evidence_sha256",
    "replacement_capacity_proven",
    "replacement_capacity_evidence_sha256",
}
WORKLOAD_CONTRACT_FIELDS = (
    "schema",
    "target",
    "iperf_port",
    "dns_target",
    "dns_name",
    "rate_cap_bps",
    "steady_tcp_forward_bps",
    "steady_tcp_reverse_bps",
    "steady_udp_reverse_bps",
    "steady_short_forward_bps",
    "steady_short_reverse_bps",
    "steady_short_connections_per_cycle",
    "quiet_tcp_forward_bps",
    "quiet_tcp_reverse_bps",
    "quiet_udp_reverse_bps",
    "quiet_short_forward_bps",
    "quiet_short_reverse_bps",
    "quiet_short_connections_per_cycle",
    "churn_tcp_forward_bps",
    "churn_tcp_reverse_bps",
    "churn_udp_reverse_bps",
    "churn_short_forward_bps",
    "churn_short_reverse_bps",
    "churn_short_connections_per_cycle",
    "tcp_reverse_iperf_length_bytes",
    "udp_payload_bytes",
    "total_secs",
    "steady_a_secs",
    "idle_secs",
    "quiet_a_secs",
    "steady_b_secs",
    "churn_secs",
    "quiet_b_secs",
    "steady_c_secs",
    "final_drain_secs",
    "tcp_epoch_secs",
    "udp_epoch_secs",
    "short_epoch_secs",
    "expected_cycles",
    "expected_tcp_results",
    "expected_udp_results",
    "expected_phase_results",
    "expected_checkpoints",
    "egress_url",
    "browser_url",
)


class LedgerError(ValueError):
    """Raised when evidence cannot enter the decision ledger."""


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LedgerError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=unique_object)


def exact_object(value: Any, fields: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LedgerError(f"{label} must be an object")
    missing = sorted(fields - value.keys())
    unknown = sorted(value.keys() - fields)
    if missing:
        raise LedgerError(f"{label} missing fields: {','.join(missing)}")
    if unknown:
        raise LedgerError(f"{label} unknown fields: {','.join(unknown)}")
    return value


def token(value: Any, label: str) -> str:
    if not isinstance(value, str) or TOKEN_RE.fullmatch(value) is None:
        raise LedgerError(f"{label} is not a safe token")
    return value


def digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
        raise LedgerError(f"{label} is not a lowercase SHA-256")
    return value


def commit(value: Any, label: str) -> str:
    if not isinstance(value, str) or COMMIT_RE.fullmatch(value) is None:
        raise LedgerError(f"{label} is not a full lowercase Git commit")
    return value


def positive_integer(value: Any, label: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise LedgerError(f"{label} must be a positive integer")
    if maximum is not None and value > maximum:
        raise LedgerError(f"{label} exceeds {maximum}")
    return value


def nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LedgerError(f"{label} must be a nonnegative integer")
    return value


def boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise LedgerError(f"{label} must be a boolean")
    return value


def ipv4(value: Any, label: str) -> str:
    if not isinstance(value, str) or IPV4_RE.fullmatch(value) is None:
        raise LedgerError(f"{label} must be IPv4")
    octets = value.split(".")
    if any(int(part) > 255 for part in octets):
        raise LedgerError(f"{label} has an invalid octet")
    return value


def public_ipv4(value: Any, label: str) -> str:
    result = ipv4(value, label)
    if not ipaddress.IPv4Address(result).is_global:
        raise LedgerError(f"{label} must be globally routable")
    return result


def utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LedgerError(f"{label} must be an exact UTC timestamp")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise LedgerError(f"{label} is not an exact UTC timestamp") from error


def percentage(value: Any, label: str) -> Decimal:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None:
        raise LedgerError(f"{label} must be a decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise LedgerError(f"{label} is invalid") from error
    if result < 0 or result > 100:
        raise LedgerError(f"{label} is outside 0..100")
    return result


def bundle_name(value: Any, label: str, allow_empty: bool = False) -> str:
    if allow_empty and value == "":
        return ""
    if not isinstance(value, str) or BUNDLE_RE.fullmatch(value) is None:
        raise LedgerError(f"{label} is not a safe bundle basename")
    return value


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256_bytes(encoded)


class EvidenceArchive:
    """Read selected regular files from one safe single-root tar archive."""

    def __init__(self, source: Path | io.BytesIO, label: str) -> None:
        self.label = label
        file_object = source if isinstance(source, io.BytesIO) else None
        file_name = None if isinstance(source, io.BytesIO) else source
        try:
            self.archive = tarfile.open(
                fileobj=file_object,
                name=file_name,
                mode="r:gz",
            )
        except tarfile.TarError as error:
            raise LedgerError(f"{label} is not a readable gzip tar archive") from error
        members = self.archive.getmembers()
        if not members or len(members) > 20_000:
            raise LedgerError(f"{label} has an invalid member count")
        roots: set[str] = set()
        self.files: dict[str, tarfile.TarInfo] = {}
        seen: set[str] = set()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or len(path.parts) < 1:
                raise LedgerError(f"{label} has an unsafe member path")
            if member.name != path.as_posix():
                raise LedgerError(f"{label} has a non-canonical member path")
            roots.add(path.parts[0])
            if member.name in seen:
                raise LedgerError(f"{label} has a duplicate member")
            seen.add(member.name)
            if member.isdir():
                continue
            if not member.isfile() or len(path.parts) < 2:
                raise LedgerError(f"{label} has a non-regular member")
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            if relative in self.files:
                raise LedgerError(f"{label} has a duplicate relative member")
            self.files[relative] = member
        if len(roots) != 1 or not TOKEN_RE.fullmatch(next(iter(roots))):
            raise LedgerError(f"{label} must have one safe root")

    def close(self) -> None:
        self.archive.close()

    def bytes(self, relative: str, maximum: int = 8 * 1024 * 1024) -> bytes:
        member = self.files.get(relative)
        if member is None or member.size > maximum:
            raise LedgerError(f"{self.label} missing or oversized: {relative}")
        extracted = self.archive.extractfile(member)
        if extracted is None:
            raise LedgerError(f"{self.label} cannot read: {relative}")
        value = extracted.read(maximum + 1)
        if len(value) > maximum:
            raise LedgerError(f"{self.label} oversized: {relative}")
        return value

    def text(self, relative: str, maximum: int = 8 * 1024 * 1024) -> str:
        try:
            return self.bytes(relative, maximum).decode("utf-8")
        except UnicodeDecodeError as error:
            raise LedgerError(f"{self.label} is not UTF-8: {relative}") from error


def parse_key_values(text: str, label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[a-z][a-z0-9_.-]*", key) or value == "":
            continue
        if key in result:
            raise LedgerError(f"{label} has duplicate key: {key}")
        result[key] = value
    return result


def parse_summary(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        match = re.fullmatch(r"- ([a-z0-9_]+): (.+)", line)
        if match is None:
            continue
        key, value = match.groups()
        if key in result:
            raise LedgerError(f"summary has duplicate key: {key}")
        result[key] = value
    return result


def required(mapping: dict[str, str], key: str, label: str) -> str:
    value = mapping.get(key)
    if value is None or value == "":
        raise LedgerError(f"{label} missing key: {key}")
    return value


def json_bytes(value: bytes, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value, object_pairs_hook=unique_object)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise LedgerError(f"{label} is invalid JSON") from error
    if not isinstance(parsed, dict):
        raise LedgerError(f"{label} must be a JSON object")
    return parsed


def resource_identity(profile: dict[str, Any]) -> str:
    try:
        identity = {field: profile[field] for field in RESOURCE_IDENTITY_FIELDS}
    except KeyError as error:
        raise LedgerError(f"candidate profile missing {error.args[0]}") from error
    return canonical_sha256(identity)


def workload_contract(profile: dict[str, str]) -> str:
    contract = {
        field: required(profile, field, "workload profile")
        for field in WORKLOAD_CONTRACT_FIELDS
    }
    return canonical_sha256(contract)


def git_command(repo: Path, arguments: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def validate_source_tree(value: Any, repo: Path) -> None:
    if not repo.is_dir() or repo.is_symlink():
        raise LedgerError("repository must be a non-symlink directory")
    if git_command(repo, ["rev-parse", "--is-inside-work-tree"]).returncode != 0:
        raise LedgerError("repository is not a Git worktree")
    ledger = exact_object(value, LEDGER_FIELDS, "ledger")
    blob_fields = {
        "scripts/knife15-macos-soak.sh": "runner_sha256",
        "scripts/knife15-m2-resource-profile.py": (
            "resource_profile_helper_sha256"
        ),
        "scripts/knife15-m2-resource-preflight.sh": (
            "resource_preflight_runner_sha256"
        ),
        "scripts/knife15-exit-target-observer.sh": "observer_sha256",
    }
    for item in ledger["attempts"]:
        if item["evidence_class"] == "invalid":
            continue
        source = item["source_commit"]
        ancestry = git_command(
            repo,
            ["merge-base", "--is-ancestor", STRICT_RESOURCE_SOURCE_FLOOR, source],
        )
        if ancestry.returncode != 0:
            raise LedgerError(f"source predates strict resource gates: {source}")
        for relative, field in blob_fields.items():
            blob = git_command(repo, ["show", f"{source}:{relative}"])
            if blob.returncode != 0:
                raise LedgerError(f"source lacks required evidence tool: {relative}")
            if sha256_bytes(blob.stdout) != item[field]:
                raise LedgerError(f"source/tool SHA-256 mismatch: {relative}")


def verify_bundle(
    artifact_root: Path,
    name: str,
    expected_sha256: str,
    label: str,
) -> None:
    path = artifact_root / name
    if not path.is_file() or path.is_symlink():
        raise LedgerError(f"{label} is missing or symlinked: {name}")
    if sha256_file(path) != expected_sha256:
        raise LedgerError(f"{label} SHA-256 mismatch: {name}")


def validate_resource_archive(mac: EvidenceArchive, stage: str) -> None:
    directory = f"{stage}-resource-preflight"
    archive_name = f"{directory}.tar.gz"
    archive_bytes = mac.bytes(archive_name, 32 * 1024 * 1024)
    checksum_text = mac.text(f"{archive_name}.sha256", 1024)
    checksum_lines = checksum_text.splitlines()
    if len(checksum_lines) != 1:
        raise LedgerError("resource archive checksum must have one line")
    checksum_fields = checksum_lines[0].split()
    if len(checksum_fields) != 2:
        raise LedgerError("resource archive checksum is malformed")
    expected = checksum_fields[0]
    if SHA_RE.fullmatch(expected) is None or sha256_bytes(archive_bytes) != expected:
        raise LedgerError("resource archive checksum mismatch")
    nested = EvidenceArchive(io.BytesIO(archive_bytes), "resource archive")
    try:
        required_names = {
            "reference-profile.json",
            "candidate-profile.json",
            "eligibility.json",
            "direct-manifest.txt",
            "provider-identity.txt",
            "route-identity.txt",
            "exit.route.txt",
            "target.route.txt",
            "exit.traceroute.txt",
            "target.traceroute.txt",
            "remote.txt",
            "remote.stderr",
            "secret-scan.txt",
            "result.txt",
            "SHA256SUMS",
        }
        if not required_names.issubset(nested.files):
            raise LedgerError("resource archive lacks required evidence")
        outer_names = {
            name.removeprefix(f"{directory}/")
            for name in mac.files
            if name.startswith(f"{directory}/")
        }
        if set(nested.files) != outer_names:
            raise LedgerError("resource directory/archive member mismatch")
        for name in outer_names:
            if nested.bytes(name) != mac.bytes(f"{directory}/{name}"):
                raise LedgerError(f"resource directory/archive differs: {name}")
        sums = nested.text("SHA256SUMS")
        summed: set[str] = set()
        for line in sums.splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9._-]+)", line)
            if match is None:
                raise LedgerError("resource SHA256SUMS has a malformed line")
            expected, name = match.groups()
            if name in summed or name == "SHA256SUMS" or name not in nested.files:
                raise LedgerError("resource SHA256SUMS has an invalid member")
            summed.add(name)
            if sha256_bytes(nested.bytes(name)) != expected:
                raise LedgerError(f"resource member checksum mismatch: {name}")
        if summed != set(nested.files) - {"SHA256SUMS"}:
            raise LedgerError("resource SHA256SUMS does not cover every member")
    finally:
        nested.close()


def validate_exit_bundle(attempt: dict[str, Any], artifact_root: Path) -> None:
    path = artifact_root / attempt["exit_bundle_name"]
    archive = EvidenceArchive(path, "Exit bundle")
    try:
        metadata = parse_key_values(archive.text("metadata.txt"), "Exit metadata")
        if required(metadata, "schema", "Exit metadata") != (
            "knife15-exit-target-observer-v2"
        ):
            raise LedgerError("Exit observer schema mismatch")
        for key, expected in (
            ("target", attempt["target_ipv4"]),
            ("iperf_port", str(attempt["target_iperf_port"])),
            ("tuic_port", str(attempt["tuic_port"])),
        ):
            if required(metadata, key, "Exit metadata") != expected:
                raise LedgerError(f"Exit observer mismatch: {key}")
        observer_started = utc(
            required(metadata, "started_at", "Exit metadata"),
            "Exit metadata.started_at",
        )
        observer_stopped = utc(
            required(metadata, "stopped_at", "Exit metadata"),
            "Exit metadata.stopped_at",
        )
        observer_frozen = utc(
            required(metadata, "frozen_at", "Exit metadata"),
            "Exit metadata.frozen_at",
        )
        if observer_stopped < observer_started or observer_frozen < observer_stopped:
            raise LedgerError("Exit observer lifecycle timestamps are out of order")
        if observer_started > utc(attempt["started_utc"], "attempt.started_utc"):
            raise LedgerError("Exit observer started after the workload")
        if observer_stopped < utc(attempt["ended_utc"], "attempt.ended_utc"):
            raise LedgerError("Exit observer stopped before the workload ended")
        secret = archive.text("secret-scan.txt", 4096).splitlines()
        if len(secret) != 1 or not secret[0].startswith("PASS:"):
            raise LedgerError("Exit secret scan did not pass")
        counters = archive.text("counters.csv", 64 * 1024 * 1024).splitlines()
        expected_header = (
            "timestamp,epoch,target_ingress_packets,target_ingress_bytes,"
            "target_egress_packets,target_egress_bytes,tuic_ingress_packets,"
            "tuic_ingress_bytes,tuic_egress_packets,tuic_egress_bytes"
        )
        if len(counters) < 3 or counters[0] != expected_header:
            raise LedgerError("Exit counters are missing or malformed")
        counter_times: list[datetime] = []
        counter_epochs: list[int] = []
        for number, line in enumerate(counters[1:], start=2):
            fields = line.split(",")
            if len(fields) != 10 or any(
                re.fullmatch(r"[0-9]+", field) is None for field in fields[1:]
            ):
                raise LedgerError(f"Exit counters have a malformed row: {number}")
            counter_times.append(utc(fields[0], f"Exit counters row {number}"))
            counter_epochs.append(int(fields[1]))
        if any(
            counter_epochs[index] <= counter_epochs[index - 1]
            for index in range(1, len(counter_epochs))
        ):
            raise LedgerError("Exit counter epochs are not strictly increasing")
        attempt_start = utc(attempt["started_utc"], "attempt.started_utc")
        attempt_end = utc(attempt["ended_utc"], "attempt.ended_utc")
        if counter_times[0] > attempt_start or counter_times[-1] < attempt_end:
            raise LedgerError("Exit counters do not cover the complete workload")
        capture_members = [
            member
            for name, member in archive.files.items()
            if name.startswith("capture.pcap")
        ]
        if not capture_members or not any(member.size > 0 for member in capture_members):
            raise LedgerError("Exit bundle has no nonempty packet capture")
        stderr = archive.text("tcpdump.stderr")
        drops = re.findall(r"^([0-9]+) packets dropped by kernel$", stderr, re.M)
        if not drops or int(drops[-1]) != 0:
            raise LedgerError("Exit capture has missing or nonzero kernel drops")
        sums = archive.text("SHA256SUMS")
        summed: set[str] = set()
        for line in sums.splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  (?:\./)?(.+)", line)
            if match is None:
                raise LedgerError("Exit SHA256SUMS has a malformed line")
            expected, name = match.groups()
            if name in summed or name == "SHA256SUMS":
                raise LedgerError("Exit SHA256SUMS has a duplicate/recursive member")
            if name not in archive.files:
                raise LedgerError(f"Exit SHA256SUMS names a missing file: {name}")
            summed.add(name)
            if archive.files[name].size <= 8 * 1024 * 1024:
                if sha256_bytes(archive.bytes(name)) != expected:
                    raise LedgerError(f"Exit member checksum mismatch: {name}")
        if summed != set(archive.files) - {"SHA256SUMS"}:
            raise LedgerError("Exit SHA256SUMS does not cover every member")
    finally:
        archive.close()


def validate_mac_bundle(attempt: dict[str, Any], artifact_root: Path) -> None:
    path = artifact_root / attempt["mac_bundle_name"]
    archive = EvidenceArchive(path, "Mac bundle")
    stage = "m2-qualification" if attempt["role"] == "qualification" else "m2"
    resource_dir = f"{stage}-resource-preflight"
    try:
        manifest = parse_key_values(archive.text("manifest.txt"), "Mac manifest")
        workload = parse_key_values(
            archive.text(f"{stage}-workload.txt"),
            "workload profile",
        )
        summary = parse_summary(archive.text("summary.md"))
        events = archive.text("events.tsv")
        candidate_bytes = archive.bytes(f"{resource_dir}/candidate-profile.json")
        candidate = json_bytes(candidate_bytes, "candidate profile")
        exact_object(candidate, RESOURCE_PROFILE_FIELDS, "candidate profile")
        if candidate.get("schema") != "knife15-m2-resource-profile-v1":
            raise LedgerError("candidate profile schema mismatch")
        eligibility = json_bytes(
            archive.bytes(f"{resource_dir}/eligibility.json"),
            "resource eligibility",
        )
        resource_result = parse_key_values(
            archive.text(f"{resource_dir}/result.txt"),
            "resource preflight result",
        )
        resource_status = archive.text(
            f"{stage}-resource-admission.status",
            1024,
        ).splitlines()
        stage_status = archive.text(f"{stage}.status", 1024).splitlines()
        observer_status = parse_key_values(
            archive.text("m2-exit-observer-status.txt"),
            "Mac observer status",
        )
        if resource_status != ["pass"]:
            raise LedgerError("Mac resource admission status is not exact PASS")
        validate_resource_archive(archive, stage)
        start_events = re.findall(
            rf"^([^\t]+)\t{re.escape(stage)} start (?:exact_cycles|planned_secs)=",
            events,
            re.M,
        )
        terminal_word = "complete" if attempt["evidence_class"] == "pass" else "failed"
        terminal_events = re.findall(
            rf"^([^\t]+)\t{re.escape(stage)} {terminal_word}(?: |$)",
            events,
            re.M,
        )
        if start_events != [attempt["started_utc"]] or terminal_events != [
            attempt["ended_utc"]
        ]:
            raise LedgerError("attempt timestamps do not match Mac stage events")

        if required(manifest, "source_commit", "Mac manifest") != attempt[
            "source_commit"
        ]:
            raise LedgerError("Mac source commit mismatch")
        if required(manifest, "binary_sha256", "Mac manifest") != attempt[
            "binary_sha256"
        ]:
            raise LedgerError("Mac binary SHA-256 mismatch")
        if required(manifest, "runner_sha256", "Mac manifest") != attempt[
            "runner_sha256"
        ]:
            raise LedgerError("Mac runner SHA-256 mismatch")
        for key, expected in (
            ("exit_host", attempt["exit_ipv4"]),
            ("exit_port", str(attempt["tuic_port"])),
            ("target", attempt["target_ipv4"]),
            ("iperf_port", str(attempt["target_iperf_port"])),
        ):
            if required(manifest, key, "Mac manifest") != expected:
                raise LedgerError(f"Mac manifest mismatch: {key}")

        if canonical_sha256(candidate) != attempt["resource_profile_sha256"]:
            raise LedgerError("resource profile SHA-256 mismatch")
        if resource_identity(candidate) != attempt["resource_identity_sha256"]:
            raise LedgerError("resource identity SHA-256 mismatch")
        for key, expected in (
            ("candidate_id", attempt["candidate_id"]),
            ("source_commit", attempt["source_commit"]),
            ("client_binary_sha256", attempt["binary_sha256"]),
            ("server_binary_sha256", attempt["server_binary_sha256"]),
            ("server_config_sha256", attempt["server_config_sha256"]),
            ("observer_sha256", attempt["observer_sha256"]),
            ("public_ipv4", attempt["exit_ipv4"]),
            ("tuic_port", attempt["tuic_port"]),
            ("target_ipv4", attempt["target_ipv4"]),
            ("target_iperf_port", attempt["target_iperf_port"]),
        ):
            if candidate.get(key) != expected:
                raise LedgerError(f"candidate profile mismatch: {key}")
        if eligibility.get("eligible") is not True or eligibility.get(
            "candidate_profile_sha256"
        ) != attempt["resource_profile_sha256"]:
            raise LedgerError("resource eligibility/profile binding mismatch")
        for key, expected in (
            ("status", "pass"),
            ("candidate_id", attempt["candidate_id"]),
            ("candidate_ipv4", attempt["exit_ipv4"]),
            ("candidate_tuic_port", str(attempt["tuic_port"])),
            ("target", attempt["target_ipv4"]),
            ("target_iperf_port", str(attempt["target_iperf_port"])),
            ("source_commit", attempt["source_commit"]),
            ("binary_sha256", attempt["binary_sha256"]),
            ("observer_sha256", attempt["observer_sha256"]),
            (
                "profile_helper_sha256",
                attempt["resource_profile_helper_sha256"],
            ),
            (
                "preflight_runner_sha256",
                attempt["resource_preflight_runner_sha256"],
            ),
        ):
            if required(resource_result, key, "resource preflight result") != expected:
                raise LedgerError(f"resource preflight mismatch: {key}")
        if required(workload, "resource_candidate_id", "workload profile") != (
            attempt["candidate_id"]
        ):
            raise LedgerError("workload candidate mismatch")
        if required(workload, "resource_profile_sha256", "workload profile") != (
            attempt["resource_profile_sha256"]
        ):
            raise LedgerError("workload resource profile mismatch")
        if workload_contract(workload) != attempt["workload_contract_sha256"]:
            raise LedgerError("workload contract SHA-256 mismatch")

        for key, expected in (
            ("status", "active"),
            ("observer_healthy", "1"),
            ("target", attempt["target_ipv4"]),
            ("iperf_port", str(attempt["target_iperf_port"])),
            ("tuic_port", str(attempt["tuic_port"])),
        ):
            if required(observer_status, key, "Mac observer status") != expected:
                raise LedgerError(f"Mac observer mismatch: {key}")
        if attempt["role"] == "formal":
            finalization = parse_key_values(
                archive.text("m2-exit-observer-finalization.txt"),
                "Mac observer finalization",
            )
            for key, expected in (
                ("observer_script_sha256", attempt["observer_sha256"]),
                ("sha256", attempt["exit_bundle_sha256"]),
                ("finalization_status", "complete"),
            ):
                if required(finalization, key, "Mac observer finalization") != expected:
                    raise LedgerError(f"observer finalization mismatch: {key}")

        if required(summary, "cleanup_evidence", "summary") != "PASS":
            raise LedgerError("Mac cleanup evidence did not pass")
        if required(summary, "stop_cleanup_complete", "summary") != "1":
            raise LedgerError("Mac stop cleanup is incomplete")
        if attempt["safety_pass"]:
            for key, expected in (
                ("endpoint_conservation", "PASS"),
                ("recovery_evidence_safety", "PASS"),
                ("network_control_evidence", "PASS"),
                ("interface_error_samples", "0"),
                ("physical_interface_error_samples", "0"),
            ):
                if required(summary, key, "summary") != expected:
                    raise LedgerError(f"Mac safety summary mismatch: {key}")
        prefix = "m2_qualification" if attempt["role"] == "qualification" else "m2"
        expected_summary = {
            f"{prefix}_resource_admission": "pass",
            f"{prefix}_resource_candidate": attempt["candidate_id"],
            f"{prefix}_resource_profile_sha256": attempt["resource_profile_sha256"],
            f"{prefix}_receiver_zero_intervals": str(
                attempt["receiver_zero_intervals"]
            ),
            f"{prefix}_udp_max_loss_percent": attempt["udp_max_loss_percent"],
            f"{prefix}_invalid_result_files": "0",
        }
        for key, expected in expected_summary.items():
            if required(summary, key, "summary") != expected:
                raise LedgerError(f"Mac summary mismatch: {key}")
        if attempt["role"] == "qualification" and attempt["evidence_class"] == "pass":
            if stage_status != ["PASS_NON_ACCEPTANCE"]:
                raise LedgerError("qualification status mismatch")
            if required(summary, "m2_qualification_verdict", "summary") != (
                "PASS_NON_ACCEPTANCE"
            ):
                raise LedgerError("qualification pass verdict mismatch")
        if attempt["role"] == "formal" and attempt["evidence_class"] == "pass":
            if stage_status != ["complete"]:
                raise LedgerError("formal status mismatch")
            if required(summary, "formal_m2_acceptance", "summary") != "PASS":
                raise LedgerError("formal pass verdict mismatch")
        if attempt["evidence_class"] == "quality_failure":
            if stage_status != ["failed"]:
                raise LedgerError("quality-failure stage status mismatch")
            verdict_key = (
                "m2_qualification_verdict"
                if attempt["role"] == "qualification"
                else "formal_m2_acceptance"
            )
            if required(summary, verdict_key, "summary") not in {"FAIL", "MISMATCH"}:
                raise LedgerError("quality-failure verdict mismatch")
        secret = archive.text("secret-scan.txt", 4096).splitlines()
        if len(secret) != 1 or not secret[0].startswith("PASS:"):
            raise LedgerError("Mac secret scan did not pass")
    finally:
        archive.close()


def validate_reference(value: Any) -> dict[str, str]:
    reference = exact_object(value, set(REFERENCE), "reference")
    if reference != REFERENCE:
        raise LedgerError("historical .33 reference is not exact")
    return reference  # type: ignore[return-value]


def validate_attempt(
    value: Any,
    artifact_root: Path,
    verify_artifacts: bool = True,
    verify_contents: bool = True,
) -> dict[str, Any]:
    attempt = exact_object(value, ATTEMPT_FIELDS, "attempt")
    if attempt["schema"] != ATTEMPT_SCHEMA:
        raise LedgerError("attempt schema mismatch")
    positive_integer(attempt["sequence"], "attempt.sequence")
    candidate_id = token(attempt["candidate_id"], "attempt.candidate_id")
    if candidate_id == REFERENCE["candidate_id"]:
        raise LedgerError("the historical .33 reference cannot consume a candidate slot")
    for field in (
        "resource_identity_sha256",
        "resource_profile_sha256",
        "binary_sha256",
        "runner_sha256",
        "resource_profile_helper_sha256",
        "resource_preflight_runner_sha256",
        "workload_contract_sha256",
        "server_binary_sha256",
        "server_config_sha256",
        "observer_sha256",
        "mac_bundle_sha256",
    ):
        digest(attempt[field], f"attempt.{field}")
    commit(attempt["source_commit"], "attempt.source_commit")
    exit_ip = public_ipv4(attempt["exit_ipv4"], "attempt.exit_ipv4")
    if exit_ip == REFERENCE["exit_ipv4"]:
        raise LedgerError("the historical .33 Exit cannot be a new candidate")
    public_ipv4(attempt["target_ipv4"], "attempt.target_ipv4")
    positive_integer(attempt["tuic_port"], "attempt.tuic_port", 65535)
    positive_integer(
        attempt["target_iperf_port"], "attempt.target_iperf_port", 65535
    )
    role = attempt["role"]
    if role not in {"qualification", "formal"}:
        raise LedgerError("attempt.role must be qualification or formal")
    evidence_class = attempt["evidence_class"]
    if evidence_class not in {"pass", "quality_failure", "invalid"}:
        raise LedgerError("attempt.evidence_class is invalid")
    started = utc(attempt["started_utc"], "attempt.started_utc")
    ended = utc(attempt["ended_utc"], "attempt.ended_utc")
    if ended <= started:
        raise LedgerError("attempt end must be after start")
    mac_name = bundle_name(attempt["mac_bundle_name"], "attempt.mac_bundle_name")
    exit_name = bundle_name(
        attempt["exit_bundle_name"],
        "attempt.exit_bundle_name",
        allow_empty=evidence_class == "invalid",
    )
    exit_sha = attempt["exit_bundle_sha256"]
    if exit_name == "":
        if exit_sha != "":
            raise LedgerError("empty Exit bundle name requires an empty SHA-256")
    else:
        digest(exit_sha, "attempt.exit_bundle_sha256")
    for field in (
        "resource_admission_pass",
        "observer_match",
        "cleanup_pass",
        "result_integrity_pass",
        "safety_pass",
        "strict_sli_pass",
    ):
        boolean(attempt[field], f"attempt.{field}")
    zeros = nonnegative_integer(
        attempt["receiver_zero_intervals"],
        "attempt.receiver_zero_intervals",
    )
    udp_loss = percentage(attempt["udp_max_loss_percent"], "attempt.udp_max_loss_percent")
    reason = attempt["reason"]

    if evidence_class == "pass":
        if reason not in PASS_REASONS:
            raise LedgerError("pass attempt has an invalid reason")
        required = (
            "resource_admission_pass",
            "observer_match",
            "cleanup_pass",
            "result_integrity_pass",
            "safety_pass",
            "strict_sli_pass",
        )
        if not all(attempt[field] is True for field in required):
            raise LedgerError("pass attempt is missing a required PASS gate")
        if zeros != 0 or udp_loss > Decimal("3.0"):
            raise LedgerError("pass attempt violates the strict SLI")
    elif evidence_class == "quality_failure":
        if reason not in QUALITY_REASONS:
            raise LedgerError("quality failure has an invalid reason")
        required = (
            "resource_admission_pass",
            "observer_match",
            "cleanup_pass",
            "result_integrity_pass",
        )
        if not all(attempt[field] is True for field in required):
            raise LedgerError("quality failure lacks valid paired evidence")
        if attempt["strict_sli_pass"] is True:
            raise LedgerError("quality failure cannot have a strict SLI pass")
        if reason == "receiver_zero" and zeros == 0:
            raise LedgerError("receiver-zero failure contains no zero interval")
        if reason == "udp_loss" and udp_loss <= Decimal("3.0"):
            raise LedgerError("UDP failure does not exceed 3 percent")
        if reason in {"safety_failure", "lifecycle_failure"} and attempt["safety_pass"]:
            raise LedgerError("safety/lifecycle failure is marked safety PASS")
    else:
        if reason not in INVALID_REASONS:
            raise LedgerError("invalid attempt has an invalid reason")
        if attempt["strict_sli_pass"]:
            raise LedgerError("invalid evidence cannot supply a strict pass")

    if verify_artifacts:
        verify_bundle(
            artifact_root,
            mac_name,
            attempt["mac_bundle_sha256"],
            "Mac bundle",
        )
        if exit_name:
            verify_bundle(artifact_root, exit_name, exit_sha, "Exit bundle")
        if evidence_class != "invalid" and verify_contents:
            if not exit_name:
                raise LedgerError("valid evidence requires a paired Exit bundle")
            validate_mac_bundle(attempt, artifact_root)
            validate_exit_bundle(attempt, artifact_root)
    return attempt


def candidate_contract(attempt: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(
        attempt[field]
        for field in (
            "candidate_id",
            "resource_identity_sha256",
            "source_commit",
            "binary_sha256",
            "runner_sha256",
            "resource_profile_helper_sha256",
            "resource_preflight_runner_sha256",
            "workload_contract_sha256",
            "server_binary_sha256",
            "server_config_sha256",
            "observer_sha256",
            "exit_ipv4",
            "tuic_port",
            "target_ipv4",
            "target_iperf_port",
        )
    )


def evaluate(
    value: Any,
    artifact_root: Path,
    verify_artifacts: bool = True,
    verify_contents: bool = True,
) -> dict[str, Any]:
    ledger = exact_object(value, LEDGER_FIELDS, "ledger")
    if ledger["schema"] != LEDGER_SCHEMA:
        raise LedgerError("ledger schema mismatch")
    validate_reference(ledger["reference"])
    raw_attempts = ledger["attempts"]
    if not isinstance(raw_attempts, list):
        raise LedgerError("ledger.attempts must be an array")
    attempts = [
        validate_attempt(item, artifact_root, verify_artifacts, verify_contents)
        for item in raw_attempts
    ]
    sequences = [attempt["sequence"] for attempt in attempts]
    if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
        raise LedgerError("attempt sequences must be unique and increasing")
    if len({attempt["mac_bundle_sha256"] for attempt in attempts}) != len(attempts):
        raise LedgerError("a Mac bundle cannot appear in more than one attempt")
    valid_attempts = [
        attempt for attempt in attempts if attempt["evidence_class"] != "invalid"
    ]
    exit_hashes = [
        attempt["exit_bundle_sha256"]
        for attempt in valid_attempts
        if attempt["exit_bundle_sha256"]
    ]
    if len(exit_hashes) != len(set(exit_hashes)):
        raise LedgerError("an Exit bundle cannot appear in more than one attempt")
    profile_hashes = [
        attempt["resource_profile_sha256"] for attempt in valid_attempts
    ]
    if len(profile_hashes) != len(set(profile_hashes)):
        raise LedgerError("each attempt requires a fresh resource evidence profile")
    if any(
        attempts[index]["started_utc"] < attempts[index - 1]["started_utc"]
        for index in range(1, len(attempts))
    ):
        raise LedgerError("attempt timestamps must be nondecreasing")

    states: dict[str, dict[str, Any]] = {}
    candidate_order: list[str] = []
    ignored_invalid: list[str] = []
    supporting_hashes: list[str] = []
    accepted_candidate: str | None = None

    for attempt in attempts:
        candidate_id = attempt["candidate_id"]
        if attempt["evidence_class"] == "invalid":
            ignored_invalid.append(attempt["mac_bundle_sha256"])
            continue
        if accepted_candidate is not None:
            raise LedgerError("valid attempt recorded after Tier A acceptance")
        if candidate_id not in states:
            if len(candidate_order) >= 2:
                raise LedgerError("more than two valid new candidates were attempted")
            if candidate_order and states[candidate_order[-1]]["state"] != "rejected":
                raise LedgerError(
                    "a new candidate started before the prior candidate was rejected"
                )
            if any(
                state["resource_identity_sha256"]
                == attempt["resource_identity_sha256"]
                for state in states.values()
            ):
                raise LedgerError("one resource identity was counted as two candidates")
            if any(
                state["exit_ipv4"] == attempt["exit_ipv4"]
                for state in states.values()
            ):
                raise LedgerError("one Exit IPv4 was counted as two candidates")
            candidate_order.append(candidate_id)
            states[candidate_id] = {
                "contract": candidate_contract(attempt),
                "resource_identity_sha256": attempt["resource_identity_sha256"],
                "exit_ipv4": attempt["exit_ipv4"],
                "state": "awaiting_qualification",
                "formal_passes": 0,
                "supporting": [],
            }
        state = states[candidate_id]
        if state["contract"] != candidate_contract(attempt):
            raise LedgerError(f"candidate contract drift: {candidate_id}")
        if state["state"] in {"rejected", "accepted"}:
            raise LedgerError(f"valid attempt after terminal candidate state: {candidate_id}")

        role = attempt["role"]
        evidence_class = attempt["evidence_class"]
        if state["state"] == "awaiting_qualification":
            if role != "qualification":
                raise LedgerError(f"formal attempt preceded qualification: {candidate_id}")
            state["supporting"].append(attempt["mac_bundle_sha256"])
            supporting_hashes.extend(
                value
                for value in (
                    attempt["mac_bundle_sha256"],
                    attempt["exit_bundle_sha256"],
                )
                if value
            )
            if evidence_class == "quality_failure":
                state["state"] = "rejected"
            else:
                state["state"] = "awaiting_formal"
            continue

        if role != "formal":
            raise LedgerError(f"a candidate may have only one valid qualification: {candidate_id}")
        state["supporting"].append(attempt["mac_bundle_sha256"])
        supporting_hashes.extend(
            value
            for value in (
                attempt["mac_bundle_sha256"],
                attempt["exit_bundle_sha256"],
            )
            if value
        )
        if evidence_class == "quality_failure":
            state["state"] = "rejected"
            state["formal_passes"] = 0
        else:
            state["formal_passes"] += 1
            if state["formal_passes"] == 2:
                state["state"] = "accepted"
                accepted_candidate = candidate_id
            elif state["formal_passes"] > 2:
                raise LedgerError(f"too many formal passes: {candidate_id}")

    if accepted_candidate is not None:
        status = "TIER_A_ACCEPTED"
    elif len(states) == 2 and all(
        state["state"] == "rejected" for state in states.values()
    ):
        status = "TIER_A_EXHAUSTED"
    else:
        status = "TIER_A_PENDING"

    public_states = [
        {
            "candidate_id": candidate_id,
            "formal_passes": states[candidate_id]["formal_passes"],
            "state": states[candidate_id]["state"],
            "supporting_mac_bundle_sha256": states[candidate_id]["supporting"],
        }
        for candidate_id in candidate_order
    ]
    return {
        "schema": RESULT_SCHEMA,
        "status": status,
        "accepted_candidate": accepted_candidate or "",
        "candidate_states": public_states,
        "ignored_invalid_mac_bundle_sha256": ignored_invalid,
        "supporting_bundle_sha256": supporting_hashes,
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def fixture_path(name: str) -> Path:
    return (
        Path(__file__).resolve().parent
        / "fixtures"
        / "knife15-m2-ledger"
        / name
    )


def materialize_scenario(value: Any, artifact_root: Path) -> dict[str, Any]:
    scenario_fields = {"schema", "name", "expected_status", "attempts"}
    scenario = exact_object(value, scenario_fields, "scenario")
    if scenario["schema"] != SCENARIO_SCHEMA:
        raise LedgerError("scenario schema mismatch")
    attempts: list[dict[str, Any]] = []
    started_hour = 0
    for sequence, item in enumerate(scenario["attempts"], start=1):
        template = exact_object(
            item,
            {"candidate", "role", "evidence_class", "reason"},
            "scenario attempt",
        )
        candidate = token(template["candidate"], "scenario candidate")
        mac_name = f"mini_vpn_knife15_macos_{scenario['name']}_{sequence}.tar.gz"
        exit_name = f"mini_vpn_knife15_exit_{scenario['name']}_{sequence}.tar.gz"
        (artifact_root / mac_name).write_bytes(f"mac:{scenario['name']}:{sequence}".encode())
        (artifact_root / exit_name).write_bytes(
            f"exit:{scenario['name']}:{sequence}".encode()
        )
        evidence_class = template["evidence_class"]
        reason = template["reason"]
        zeros = 1 if reason == "receiver_zero" else 0
        udp_loss = "3.500000" if reason == "udp_loss" else "1.000000"
        safety_pass = reason not in {"safety_failure", "lifecycle_failure"}
        strict_pass = evidence_class == "pass"
        attempts.append(
            {
                "schema": ATTEMPT_SCHEMA,
                "sequence": sequence,
                "candidate_id": candidate,
                "resource_identity_sha256": hashlib.sha256(
                    f"resource:{candidate}".encode()
                ).hexdigest(),
                "resource_profile_sha256": hashlib.sha256(
                    f"profile:{scenario['name']}:{sequence}".encode()
                ).hexdigest(),
                "source_commit": "1" * 40,
                "binary_sha256": "2" * 64,
                "runner_sha256": "d" * 64,
                "resource_profile_helper_sha256": "e" * 64,
                "resource_preflight_runner_sha256": "f" * 64,
                "workload_contract_sha256": "3" * 64,
                "server_binary_sha256": "4" * 64,
                "server_config_sha256": "5" * 64,
                "observer_sha256": "6" * 64,
                "exit_ipv4": "1.1.1.1" if candidate == "candidate-a" else "8.8.4.4",
                "tuic_port": 8443,
                "target_ipv4": "43.130.32.77",
                "target_iperf_port": 5201,
                "role": template["role"],
                "evidence_class": evidence_class,
                "reason": reason,
                "started_utc": (
                    f"2026-08-{15 + started_hour // 24:02d}"
                    f"T{started_hour % 24:02d}:00:00Z"
                ),
                "ended_utc": (
                    f"2026-08-{15 + (started_hour + 1) // 24:02d}"
                    f"T{(started_hour + 1) % 24:02d}:00:00Z"
                ),
                "mac_bundle_name": mac_name,
                "mac_bundle_sha256": sha256_file(artifact_root / mac_name),
                "exit_bundle_name": exit_name,
                "exit_bundle_sha256": sha256_file(artifact_root / exit_name),
                "resource_admission_pass": evidence_class != "invalid",
                "observer_match": evidence_class != "invalid",
                "cleanup_pass": evidence_class != "invalid",
                "result_integrity_pass": evidence_class != "invalid",
                "safety_pass": safety_pass and evidence_class != "invalid",
                "strict_sli_pass": strict_pass,
                "receiver_zero_intervals": zeros,
                "udp_max_loss_percent": udp_loss,
            }
        )
        started_hour += 2
    return {"schema": LEDGER_SCHEMA, "reference": REFERENCE, "attempts": attempts}


def write_test_archive(path: Path, root: str, files: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        root_info = tarfile.TarInfo(f"{root}/")
        root_info.type = tarfile.DIRTYPE
        root_info.mode = 0o700
        root_info.mtime = 0
        archive.addfile(root_info)
        directories: set[str] = set()
        for name in files:
            parent = PurePosixPath(name).parent
            while parent != PurePosixPath("."):
                directories.add(parent.as_posix())
                parent = parent.parent
        for directory in sorted(directories):
            info = tarfile.TarInfo(f"{root}/{directory}/")
            info.type = tarfile.DIRTYPE
            info.mode = 0o700
            info.mtime = 0
            archive.addfile(info)
        for name, value in sorted(files.items()):
            info = tarfile.TarInfo(f"{root}/{name}")
            info.size = len(value)
            info.mode = 0o600
            info.mtime = 0
            archive.addfile(info, io.BytesIO(value))


def key_value_bytes(values: dict[str, Any]) -> bytes:
    return "".join(f"{key}={value}\n" for key, value in values.items()).encode()


def make_valid_evidence_attempt(artifact_root: Path, role: str) -> dict[str, Any]:
    candidate_id = f"candidate-parser-{role}"
    source_commit = "1" * 40
    binary_sha = "2" * 64
    observer_sha = "6" * 64
    server_binary_sha = "4" * 64
    server_config_sha = "5" * 64
    direct_manifest = b"schema=knife15-macos-direct-continuity-v1\nstatus=pass\n"
    direct_sha = sha256_bytes(direct_manifest)
    candidate: dict[str, Any] = {
        "schema": "knife15-m2-resource-profile-v1",
        "candidate_id": candidate_id,
        "provider": "provider-parser",
        "resource_id": "resource-parser",
        "region": "us-parser",
        "public_ipv4": "1.1.1.1",
        "asn": 13335,
        "route_class": "premium-parser",
        "route_contract_id": "contract-parser",
        "provider_identity_evidence_sha256": "7" * 64,
        "route_identity_evidence_sha256": "8" * 64,
        "tuic_port": 8443,
        "target_ipv4": "43.130.32.77",
        "target_iperf_port": 5201,
        "server_binary_sha256": server_binary_sha,
        "server_config_sha256": server_config_sha,
        "observer_sha256": observer_sha,
        "source_commit": source_commit,
        "client_binary_sha256": binary_sha,
        "workload_profile_sha256": direct_sha,
        "mac_interface": "en0",
        "prior_saturation_proven": False,
        "prior_saturation_evidence_sha256": "",
        "replacement_capacity_proven": False,
        "replacement_capacity_evidence_sha256": "",
    }
    profile_sha = canonical_sha256(candidate)
    eligibility = {
        "schema": "knife15-m2-resource-eligibility-v1",
        "eligible": True,
        "reason": "distinct_provider_and_asn",
        "reference_id": "reference-33",
        "candidate_id": candidate_id,
        "reference_profile_sha256": "c" * 64,
        "candidate_profile_sha256": profile_sha,
    }
    resource_result = {
        "schema": "knife15-m2-resource-preflight-v1",
        "status": "pass",
        "candidate_id": candidate_id,
        "candidate_ipv4": "1.1.1.1",
        "candidate_tuic_port": 8443,
        "target": "43.130.32.77",
        "target_iperf_port": 5201,
        "source_commit": source_commit,
        "binary_sha256": binary_sha,
        "observer_sha256": observer_sha,
        "profile_helper_sha256": "e" * 64,
        "preflight_runner_sha256": "f" * 64,
    }
    resource_files: dict[str, bytes] = {
        "reference-profile.json": b"{}\n",
        "candidate-profile.json": (canonical_json(candidate) + "\n").encode(),
        "eligibility.json": (canonical_json(eligibility) + "\n").encode(),
        "direct-manifest.txt": direct_manifest,
        "provider-identity.txt": b"provider=fixture\n",
        "route-identity.txt": b"route=fixture\n",
        "exit.route.txt": b"interface: en0\n",
        "target.route.txt": b"interface: en0\n",
        "exit.traceroute.txt": b"exit traceroute\n",
        "target.traceroute.txt": b"target traceroute\n",
        "remote.txt": b"schema=knife15-m2-resource-remote-v1\n",
        "remote.stderr": b"",
        "secret-scan.txt": b"PASS: no credential-like assignment found\n",
        "result.txt": key_value_bytes(resource_result),
    }
    resource_sums = "".join(
        f"{sha256_bytes(value)}  {name}\n"
        for name, value in sorted(resource_files.items())
    ).encode()
    resource_files["SHA256SUMS"] = resource_sums
    resource_buffer = io.BytesIO()
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as temporary:
        temporary_path = Path(temporary.name)
        write_test_archive(
            temporary_path,
            "mini_vpn_knife15_resource_parser_fixture",
            resource_files,
        )
        resource_archive = temporary_path.read_bytes()

    exit_name = f"mini_vpn_knife15_exit_parser_{role}.tar.gz"
    exit_path = artifact_root / exit_name
    exit_files: dict[str, bytes] = {
        "metadata.txt": key_value_bytes(
            {
                "schema": "knife15-exit-target-observer-v2",
                "started_at": "2026-08-14T23:59:00Z",
                "target": "43.130.32.77",
                "iperf_port": 5201,
                "tuic_port": 8443,
                "stopped_at": "2026-08-15T01:00:00Z",
                "frozen_at": "2026-08-15T01:00:01Z",
            }
        ),
        "counters.csv": (
            b"timestamp,epoch,target_ingress_packets,target_ingress_bytes,"
            b"target_egress_packets,target_egress_bytes,tuic_ingress_packets,"
            b"tuic_ingress_bytes,tuic_egress_packets,tuic_egress_bytes\n"
            b"2026-08-14T23:59:00Z,1786751940,1,100,1,100,1,100,1,100\n"
            b"2026-08-15T01:00:00Z,1786755600,2,200,2,200,2,200,2,200\n"
        ),
        "tcpdump.stderr": b"10 packets captured\n0 packets dropped by kernel\n",
        "secret-scan.txt": b"PASS: no credential names found\n",
        "capture.pcap00": b"fixture capture",
    }
    exit_sums = "".join(
        f"{sha256_bytes(value)}  ./{name}\n"
        for name, value in sorted(exit_files.items())
    ).encode()
    exit_files["SHA256SUMS"] = exit_sums
    write_test_archive(exit_path, f"mini_vpn_knife15_exit_parser_{role}", exit_files)
    exit_sha = sha256_file(exit_path)

    workload_values = {
        "schema": "knife15-macos-m2-v1",
        "target": "43.130.32.77",
        "iperf_port": "5201",
        "dns_target": "8.8.8.8",
        "dns_name": "example.com",
        "rate_cap_bps": "200000000",
        "steady_tcp_forward_bps": "10000000",
        "steady_tcp_reverse_bps": "20000000",
        "steady_udp_reverse_bps": "20000000",
        "steady_short_forward_bps": "16000000",
        "steady_short_reverse_bps": "32000000",
        "steady_short_connections_per_cycle": "6",
        "quiet_tcp_forward_bps": "5000000",
        "quiet_tcp_reverse_bps": "10000000",
        "quiet_udp_reverse_bps": "10000000",
        "quiet_short_forward_bps": "10000000",
        "quiet_short_reverse_bps": "20000000",
        "quiet_short_connections_per_cycle": "6",
        "churn_tcp_forward_bps": "5000000",
        "churn_tcp_reverse_bps": "10000000",
        "churn_udp_reverse_bps": "10000000",
        "churn_short_forward_bps": "10000000",
        "churn_short_reverse_bps": "20000000",
        "churn_short_connections_per_cycle": "24",
        "tcp_reverse_iperf_length_bytes": "1024",
        "udp_payload_bytes": "1160",
        "total_secs": "86400",
        "steady_a_secs": "14400",
        "idle_secs": "600",
        "quiet_a_secs": "10800",
        "steady_b_secs": "14400",
        "churn_secs": "10800",
        "quiet_b_secs": "10800",
        "steady_c_secs": "21600",
        "final_drain_secs": "600",
        "tcp_epoch_secs": "300",
        "udp_epoch_secs": "180",
        "short_epoch_secs": "10",
        "expected_cycles": "93",
        "expected_tcp_results": "934",
        "expected_udp_results": "95",
        "expected_phase_results": "1029",
        "expected_checkpoints": "6",
        "egress_url": "https://api.ipify.org",
        "browser_url": "https://example.com/",
        "resource_candidate_id": candidate_id,
        "resource_profile_sha256": profile_sha,
    }
    contract_sha = workload_contract(workload_values)
    stage = "m2-qualification" if role == "qualification" else "m2"
    prefix = "m2_qualification" if role == "qualification" else "m2"
    summary_values = {
        "endpoint_conservation": "PASS",
        "recovery_evidence_safety": "PASS",
        "network_control_evidence": "PASS",
        "interface_error_samples": "0",
        "physical_interface_error_samples": "0",
        "stop_cleanup_complete": "1",
        "cleanup_evidence": "PASS",
        f"{prefix}_resource_admission": "pass",
        f"{prefix}_resource_candidate": candidate_id,
        f"{prefix}_resource_profile_sha256": profile_sha,
        f"{prefix}_receiver_zero_intervals": "0",
        f"{prefix}_udp_max_loss_percent": "1.000000",
        f"{prefix}_invalid_result_files": "0",
    }
    if role == "qualification":
        summary_values["m2_qualification_verdict"] = "PASS_NON_ACCEPTANCE"
    else:
        summary_values["formal_m2_acceptance"] = "PASS"
    summary = "# fixture\n\n" + "".join(
        f"- {key}: {value}\n" for key, value in summary_values.items()
    )
    mac_files: dict[str, bytes] = {
        "manifest.txt": key_value_bytes(
            {
                "source_commit": source_commit,
                "binary_sha256": binary_sha,
                "runner_sha256": "d" * 64,
                "exit_host": "1.1.1.1",
                "exit_port": 8443,
                "target": "43.130.32.77",
                "iperf_port": 5201,
            }
        ),
        f"{stage}-workload.txt": key_value_bytes(workload_values),
        f"{stage}-resource-admission.status": b"pass\n",
        f"{stage}.status": (
            b"PASS_NON_ACCEPTANCE\n" if role == "qualification" else b"complete\n"
        ),
        "summary.md": summary.encode(),
        "events.tsv": (
            f"timestamp\tevent\n"
            f"2026-08-15T00:00:00Z\t{stage} start planned_secs=fixture\n"
            f"2026-08-15T01:00:00Z\t{stage} complete fixture\n"
        ).encode(),
        "m2-exit-observer-status.txt": key_value_bytes(
            {
                "status": "active",
                "observer_healthy": 1,
                "target": "43.130.32.77",
                "iperf_port": 5201,
                "tuic_port": 8443,
            }
        ),
        "secret-scan.txt": b"PASS: no credential names found\n",
        f"{stage}-resource-preflight.tar.gz": resource_archive,
        f"{stage}-resource-preflight.tar.gz.sha256": (
            f"{sha256_bytes(resource_archive)}  fixture.tar.gz\n".encode()
        ),
    }
    for name, value in resource_files.items():
        mac_files[f"{stage}-resource-preflight/{name}"] = value
    if role == "formal":
        mac_files["m2-exit-observer-finalization.txt"] = key_value_bytes(
            {
                "observer_script_sha256": observer_sha,
                "sha256": exit_sha,
                "finalization_status": "complete",
            }
        )
    mac_name = f"mini_vpn_knife15_macos_parser_{role}.tar.gz"
    mac_path = artifact_root / mac_name
    write_test_archive(mac_path, f"mini_vpn_knife15_macos_parser_{role}", mac_files)
    return {
        "schema": ATTEMPT_SCHEMA,
        "sequence": 100 if role == "qualification" else 101,
        "candidate_id": candidate_id,
        "resource_identity_sha256": resource_identity(candidate),
        "resource_profile_sha256": profile_sha,
        "source_commit": source_commit,
        "binary_sha256": binary_sha,
        "runner_sha256": "d" * 64,
        "resource_profile_helper_sha256": "e" * 64,
        "resource_preflight_runner_sha256": "f" * 64,
        "workload_contract_sha256": contract_sha,
        "server_binary_sha256": server_binary_sha,
        "server_config_sha256": server_config_sha,
        "observer_sha256": observer_sha,
        "exit_ipv4": "1.1.1.1",
        "tuic_port": 8443,
        "target_ipv4": "43.130.32.77",
        "target_iperf_port": 5201,
        "role": role,
        "evidence_class": "pass",
        "reason": "strict_pass",
        "started_utc": "2026-08-15T00:00:00Z",
        "ended_utc": "2026-08-15T01:00:00Z",
        "mac_bundle_name": mac_name,
        "mac_bundle_sha256": sha256_file(mac_path),
        "exit_bundle_name": exit_name,
        "exit_bundle_sha256": exit_sha,
        "resource_admission_pass": True,
        "observer_match": True,
        "cleanup_pass": True,
        "result_integrity_pass": True,
        "safety_pass": True,
        "strict_sli_pass": True,
        "receiver_zero_intervals": 0,
        "udp_max_loss_percent": "1.000000",
    }


def derive_attempt(
    artifact_root: Path,
    sequence: int,
    role: str,
    evidence_class: str,
    reason: str,
    mac_name: str,
    mac_sha: str,
    exit_name: str,
    exit_sha: str,
) -> dict[str, Any]:
    positive_integer(sequence, "sequence")
    if role not in {"qualification", "formal"}:
        raise LedgerError("role must be qualification or formal")
    if evidence_class not in {"pass", "quality_failure"}:
        raise LedgerError("only complete paired evidence can be sealed automatically")
    bundle_name(mac_name, "Mac bundle name")
    bundle_name(exit_name, "Exit bundle name")
    digest(mac_sha, "Mac bundle SHA-256")
    digest(exit_sha, "Exit bundle SHA-256")
    verify_bundle(artifact_root, mac_name, mac_sha, "Mac bundle")
    verify_bundle(artifact_root, exit_name, exit_sha, "Exit bundle")

    stage = "m2-qualification" if role == "qualification" else "m2"
    prefix = "m2_qualification" if role == "qualification" else "m2"
    resource_dir = f"{stage}-resource-preflight"
    archive = EvidenceArchive(artifact_root / mac_name, "Mac bundle")
    try:
        manifest = parse_key_values(archive.text("manifest.txt"), "Mac manifest")
        workload = parse_key_values(
            archive.text(f"{stage}-workload.txt"),
            "workload profile",
        )
        summary = parse_summary(archive.text("summary.md"))
        candidate = json_bytes(
            archive.bytes(f"{resource_dir}/candidate-profile.json"),
            "candidate profile",
        )
        resource_result = parse_key_values(
            archive.text(f"{resource_dir}/result.txt"),
            "resource preflight result",
        )
        observer_status = parse_key_values(
            archive.text("m2-exit-observer-status.txt"),
            "Mac observer status",
        )
        events = archive.text("events.tsv")
        start_events = re.findall(
            rf"^([^\t]+)\t{re.escape(stage)} start (?:exact_cycles|planned_secs)=",
            events,
            re.M,
        )
        terminal_word = "complete" if evidence_class == "pass" else "failed"
        terminal_events = re.findall(
            rf"^([^\t]+)\t{re.escape(stage)} {terminal_word}(?: |$)",
            events,
            re.M,
        )
        if len(start_events) != 1 or len(terminal_events) != 1:
            raise LedgerError("Mac stage does not have one start and terminal event")

        candidate_id = required(workload, "resource_candidate_id", "workload")
        profile_sha = required(workload, "resource_profile_sha256", "workload")
        exit_ip = required(manifest, "exit_host", "Mac manifest")
        target = required(manifest, "target", "Mac manifest")
        tuic_port = int(required(manifest, "exit_port", "Mac manifest"))
        iperf_port = int(required(manifest, "iperf_port", "Mac manifest"))
        zeros = int(
            required(summary, f"{prefix}_receiver_zero_intervals", "summary")
        )
        udp_loss = required(summary, f"{prefix}_udp_max_loss_percent", "summary")
        safety_pairs = (
            ("endpoint_conservation", "PASS"),
            ("recovery_evidence_safety", "PASS"),
            ("network_control_evidence", "PASS"),
            ("interface_error_samples", "0"),
            ("physical_interface_error_samples", "0"),
        )
        safety_pass = all(
            summary.get(key) == expected for key, expected in safety_pairs
        )
        cleanup_pass = (
            summary.get("cleanup_evidence") == "PASS"
            and summary.get("stop_cleanup_complete") == "1"
        )
        observer_match = all(
            observer_status.get(key) == expected
            for key, expected in (
                ("status", "active"),
                ("observer_healthy", "1"),
                ("target", target),
                ("iperf_port", str(iperf_port)),
                ("tuic_port", str(tuic_port)),
            )
        )
        resource_admission_pass = (
            archive.text(f"{stage}-resource-admission.status", 1024).splitlines()
            == ["pass"]
            and summary.get(f"{prefix}_resource_admission") == "pass"
        )
        result_integrity_pass = (
            summary.get(f"{prefix}_invalid_result_files") == "0"
        )
        attempt = {
            "schema": ATTEMPT_SCHEMA,
            "sequence": sequence,
            "candidate_id": candidate_id,
            "resource_identity_sha256": resource_identity(candidate),
            "resource_profile_sha256": profile_sha,
            "source_commit": required(manifest, "source_commit", "Mac manifest"),
            "binary_sha256": required(manifest, "binary_sha256", "Mac manifest"),
            "runner_sha256": required(manifest, "runner_sha256", "Mac manifest"),
            "resource_profile_helper_sha256": required(
                resource_result,
                "profile_helper_sha256",
                "resource preflight result",
            ),
            "resource_preflight_runner_sha256": required(
                resource_result,
                "preflight_runner_sha256",
                "resource preflight result",
            ),
            "workload_contract_sha256": workload_contract(workload),
            "server_binary_sha256": candidate.get("server_binary_sha256"),
            "server_config_sha256": candidate.get("server_config_sha256"),
            "observer_sha256": candidate.get("observer_sha256"),
            "exit_ipv4": exit_ip,
            "tuic_port": tuic_port,
            "target_ipv4": target,
            "target_iperf_port": iperf_port,
            "role": role,
            "evidence_class": evidence_class,
            "reason": reason,
            "started_utc": start_events[0],
            "ended_utc": terminal_events[0],
            "mac_bundle_name": mac_name,
            "mac_bundle_sha256": mac_sha,
            "exit_bundle_name": exit_name,
            "exit_bundle_sha256": exit_sha,
            "resource_admission_pass": resource_admission_pass,
            "observer_match": observer_match,
            "cleanup_pass": cleanup_pass,
            "result_integrity_pass": result_integrity_pass,
            "safety_pass": safety_pass,
            "strict_sli_pass": evidence_class == "pass",
            "receiver_zero_intervals": zeros,
            "udp_max_loss_percent": udp_loss,
        }
    finally:
        archive.close()
    validate_attempt(attempt, artifact_root)
    return attempt


def self_test() -> None:
    names = (
        "invalid-environment.json",
        "qualification-failure.json",
        "one-formal-pass.json",
        "pass-invalid-pass.json",
        "two-formal-passes.json",
        "two-rejected-candidates.json",
    )
    with tempfile.TemporaryDirectory(prefix="knife15-m2-ledger-") as tmp:
        artifact_root = Path(tmp)
        empty = read_json(fixture_path("ledger-template.json"))
        assert evaluate(empty, artifact_root)["status"] == "TIER_A_PENDING"
        ledgers: dict[str, dict[str, Any]] = {}
        for name in names:
            scenario = read_json(fixture_path(name))
            ledger = materialize_scenario(scenario, artifact_root)
            result = evaluate(ledger, artifact_root, verify_contents=False)
            if result["status"] != scenario["expected_status"]:
                raise AssertionError(f"unexpected result for {name}: {result}")
            ledgers[name] = ledger

        qualification_attempt = make_valid_evidence_attempt(
            artifact_root,
            "qualification",
        )
        validate_attempt(qualification_attempt, artifact_root)
        formal_attempt = make_valid_evidence_attempt(artifact_root, "formal")
        validate_attempt(formal_attempt, artifact_root)
        derived_formal = derive_attempt(
            artifact_root,
            formal_attempt["sequence"],
            "formal",
            "pass",
            "strict_pass",
            formal_attempt["mac_bundle_name"],
            formal_attempt["mac_bundle_sha256"],
            formal_attempt["exit_bundle_name"],
            formal_attempt["exit_bundle_sha256"],
        )
        assert derived_formal == formal_attempt
        forged = json.loads(canonical_json(formal_attempt))
        forged["candidate_id"] = "candidate-forged"
        try:
            validate_attempt(forged, artifact_root)
        except LedgerError:
            pass
        else:
            raise AssertionError("bundle-content candidate mismatch was accepted")

        invalid_result = evaluate(
            ledgers["invalid-environment.json"],
            artifact_root,
            verify_contents=False,
        )
        assert invalid_result["candidate_states"] == []
        assert len(invalid_result["ignored_invalid_mac_bundle_sha256"]) == 1
        repo = Path(__file__).resolve().parent.parent
        source_ledger = json.loads(
            canonical_json(ledgers["qualification-failure.json"])
        )
        source_attempt = source_ledger["attempts"][0]
        head = git_command(repo, ["rev-parse", "HEAD"])
        if head.returncode != 0:
            raise AssertionError("cannot identify the self-test source commit")
        source_attempt["source_commit"] = head.stdout.decode().strip()
        for relative, field in (
            ("scripts/knife15-macos-soak.sh", "runner_sha256"),
            (
                "scripts/knife15-m2-resource-profile.py",
                "resource_profile_helper_sha256",
            ),
            (
                "scripts/knife15-m2-resource-preflight.sh",
                "resource_preflight_runner_sha256",
            ),
            ("scripts/knife15-exit-target-observer.sh", "observer_sha256"),
        ):
            blob = git_command(repo, ["show", f"HEAD:{relative}"])
            if blob.returncode != 0:
                raise AssertionError(f"cannot read self-test source: {relative}")
            source_attempt[field] = sha256_bytes(blob.stdout)
        validate_source_tree(source_ledger, repo)
        source_attempt["runner_sha256"] = "0" * 64
        try:
            validate_source_tree(source_ledger, repo)
        except LedgerError:
            pass
        else:
            raise AssertionError("source/tool hash mismatch was accepted")
        accepted = evaluate(
            ledgers["two-formal-passes.json"],
            artifact_root,
            verify_contents=False,
        )
        assert accepted["accepted_candidate"] == "candidate-a"
        accepted_across_invalid = evaluate(
            ledgers["pass-invalid-pass.json"],
            artifact_root,
            verify_contents=False,
        )
        assert accepted_across_invalid["accepted_candidate"] == "candidate-a"
        invalid_reuses_evidence = json.loads(
            canonical_json(ledgers["pass-invalid-pass.json"])
        )
        invalid_reuses_evidence["attempts"][2]["resource_profile_sha256"] = (
            invalid_reuses_evidence["attempts"][1]["resource_profile_sha256"]
        )
        invalid_reuses_evidence["attempts"][2]["exit_bundle_name"] = (
            invalid_reuses_evidence["attempts"][1]["exit_bundle_name"]
        )
        invalid_reuses_evidence["attempts"][2]["exit_bundle_sha256"] = (
            invalid_reuses_evidence["attempts"][1]["exit_bundle_sha256"]
        )
        assert evaluate(
            invalid_reuses_evidence,
            artifact_root,
            verify_contents=False,
        )["status"] == "TIER_A_ACCEPTED"
        exhausted = evaluate(
            ledgers["two-rejected-candidates.json"],
            artifact_root,
            verify_contents=False,
        )
        assert len(exhausted["candidate_states"]) == 2

        tampered = json.loads(canonical_json(ledgers["two-formal-passes.json"]))
        first_name = tampered["attempts"][0]["mac_bundle_name"]
        (artifact_root / first_name).write_bytes(b"tampered")
        try:
            evaluate(tampered, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError("tampered Mac bundle was accepted")
        (artifact_root / first_name).write_bytes(b"mac:two_formal_passes:1")

        drift = json.loads(canonical_json(ledgers["one-formal-pass.json"]))
        drift["attempts"][1]["source_commit"] = "9" * 40
        try:
            evaluate(drift, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError("candidate contract drift was accepted")

        duplicate_resource = json.loads(
            canonical_json(ledgers["two-rejected-candidates.json"])
        )
        duplicate_resource["attempts"][1]["resource_identity_sha256"] = (
            duplicate_resource["attempts"][0]["resource_identity_sha256"]
        )
        duplicate_resource["attempts"][2]["resource_identity_sha256"] = (
            duplicate_resource["attempts"][0]["resource_identity_sha256"]
        )
        try:
            evaluate(duplicate_resource, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError("one resource counted as two candidates")

        overlapping_candidates = json.loads(
            canonical_json(ledgers["one-formal-pass.json"])
        )
        early_second = json.loads(
            canonical_json(ledgers["two-rejected-candidates.json"])
        )["attempts"][1]
        early_second["sequence"] = 3
        early_second["started_utc"] = "2026-08-16T00:00:00Z"
        early_second["ended_utc"] = "2026-08-16T01:00:00Z"
        overlapping_candidates["attempts"].append(early_second)
        try:
            evaluate(overlapping_candidates, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError("overlapping strict candidates were accepted")

        post_acceptance = json.loads(
            canonical_json(ledgers["two-formal-passes.json"])
        )
        late = json.loads(
            canonical_json(ledgers["two-rejected-candidates.json"])
        )["attempts"][1]
        late["sequence"] = 4
        late["started_utc"] = "2026-08-16T00:00:00Z"
        late["ended_utc"] = "2026-08-16T01:00:00Z"
        post_acceptance["attempts"].append(late)
        try:
            evaluate(post_acceptance, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError("valid attempt after Tier A acceptance was accepted")

        duplicate = canonical_json(ledgers["qualification-failure.json"])
        duplicate = duplicate.replace(
            '"schema":"knife15-m2-tier-a-ledger-v1"',
            '"schema":"knife15-m2-tier-a-ledger-v1","schema":"duplicate"',
            1,
        )
        duplicate_path = artifact_root / "duplicate.json"
        duplicate_path.write_text(duplicate, encoding="utf-8")
        try:
            read_json(duplicate_path)
        except LedgerError:
            pass
        else:
            raise AssertionError("duplicate ledger key was accepted")
    print("knife15 M2 continuity ledger self-test passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    evaluate_parser = subparsers.add_parser("evaluate")
    evaluate_parser.add_argument("ledger", type=Path)
    evaluate_parser.add_argument("--artifact-root", type=Path, required=True)
    evaluate_parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
    )
    evaluate_parser.add_argument("--output", type=Path)
    seal_parser = subparsers.add_parser("seal-attempt")
    seal_parser.add_argument("--artifact-root", type=Path, required=True)
    seal_parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    seal_parser.add_argument("--sequence", type=int, required=True)
    seal_parser.add_argument("--role", choices=("qualification", "formal"), required=True)
    seal_parser.add_argument(
        "--evidence-class",
        choices=("pass", "quality_failure"),
        required=True,
    )
    seal_parser.add_argument(
        "--reason",
        choices=tuple(sorted(PASS_REASONS | QUALITY_REASONS)),
        required=True,
    )
    seal_parser.add_argument("--mac-bundle", required=True)
    seal_parser.add_argument("--mac-sha256", required=True)
    seal_parser.add_argument("--exit-bundle", required=True)
    seal_parser.add_argument("--exit-sha256", required=True)
    seal_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact_root = args.artifact_root.resolve(strict=True)
    if not artifact_root.is_dir():
        raise LedgerError("artifact root must resolve to a directory")
    if args.command == "seal-attempt":
        result = derive_attempt(
            artifact_root,
            args.sequence,
            args.role,
            args.evidence_class,
            args.reason,
            args.mac_bundle,
            args.mac_sha256,
            args.exit_bundle,
            args.exit_sha256,
        )
        validate_source_tree(
            {"schema": LEDGER_SCHEMA, "reference": REFERENCE, "attempts": [result]},
            args.repo,
        )
        encoded = canonical_json(result) + "\n"
    else:
        ledger = read_json(args.ledger)
        result = evaluate(ledger, artifact_root)
        validate_source_tree(ledger, args.repo)
        encoded = canonical_json(result) + "\n"
    if args.output is None:
        sys.stdout.write(encoded)
    elif args.output.exists():
        raise LedgerError("refuse to replace an existing ledger result")
    else:
        args.output.write_text(encoded, encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        AssertionError,
        json.JSONDecodeError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
