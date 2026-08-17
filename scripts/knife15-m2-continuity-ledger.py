#!/usr/bin/env python3
"""Validate and reduce the bounded Knife15 Tier-A strict-attempt ledger."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
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
TIER_A_EXHAUSTED_LEDGER_SHA256 = (
    "96e50cd21bc9cc277596d82e90d4c7fc0a5856c24e05f94d58ab8b3dec7b1989"
)
TIER_A_EXHAUSTED_EVALUATION_SHA256 = (
    "8a95308964bb464fcaef1686eaebc7627839f269b174960365d0be4c0c2d8733"
)
FREQUENCY_LEDGER_SCHEMA = "knife15-m2-frequency-ledger-v1"
FREQUENCY_RECORD_SCHEMA = "knife15-m2-frequency-sealed-epoch-v1"
FREQUENCY_RAW_EPOCH_SCHEMA = "knife15-m2-frequency-raw-epoch-v1"
FREQUENCY_LEDGER_RESULT_SCHEMA = "knife15-m2-frequency-ledger-result-v1"
FREQUENCY_SAFETY_SCHEMA = "knife15-m2-frequency-epoch-safety-v1"
FREQUENCY_SOURCE_FLOOR = "f32f624"
FREQUENCY_LEDGER_FIELDS = {
    "schema",
    "tier_a_ledger_sha256",
    "tier_a_evaluation_sha256",
    "epochs",
}
FREQUENCY_RECORD_FIELDS = {
    "schema",
    "sequence",
    "raw_epoch",
    "raw_epoch_sha256",
    "mac_bundle_name",
    "mac_bundle_sha256",
    "exit_bundle_name",
    "exit_bundle_sha256",
}
FREQUENCY_RAW_EPOCH_FIELDS = {
    "schema",
    "epoch_id",
    "run_id",
    "run_epoch_index",
    "started_utc",
    "ended_utc",
    "started_monotonic_ns",
    "ended_monotonic_ns",
    "identity",
    "tcp_results",
    "safety",
}
FREQUENCY_SAFETY_FIELDS = {
    "schema",
    "status",
    "tcp_results",
    "udp_results",
    "tcp_max_gap_bytes",
    "udp_max_loss_percent",
    "invalid_result_files",
    "dns_results",
    "invalid_dns_results",
    "real_client_results",
    "invalid_real_client_results",
    "process_samples",
    "network_samples",
    "endpoint_samples",
    "endpoint_conservation_max_bytes",
    "endpoint_live_bytes",
    "endpoint_outstanding_bytes",
    "resource_binding_pass",
    "observer_match",
    "network_control_pass",
    "d16_terminal_ownership_pass",
    "recovery_safety_pass",
    "interface_error_samples",
    "physical_interface_error_samples",
    "log_compactions",
    "internal_failure_matches",
}
STRICT_RESOURCE_SOURCE_FLOOR = "218467b"
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
RESOURCE_REFERENCE = {
    "schema": "knife15-m2-resource-reference-v1",
    "candidate_id": "reference-33",
    "provider": "tencent-cloud",
    "resource_id": "reference-exit-33",
    "region": "us-west-reference",
    "public_ipv4": "43.153.32.33",
    "asn": 132203,
    "route_class": "public-internet",
    "route_contract_id": "reference-default",
    "tuic_port": 8443,
    "target_ipv4": "43.130.32.77",
    "target_iperf_port": 5201,
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
CHECKPOINT_READER_BRIDGE_BINARY_SHA256 = (
    "5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032"
)
CHECKPOINT_READER_BRIDGE_RUNNERS = {
    "c55dc940d974539f98e2f449387159965972249dcda476fad451c44d3466e192",
    "8b0d0c9ed220075481f4459dbfbde7d8ef88738d9ca6f2afc802b65d96f25a86",
}
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
    "mac_interface",
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
PROVIDER_EVIDENCE_FIELDS = {
    "schema",
    "candidate_id",
    "provider",
    "resource_id",
    "region",
    "public_ipv4",
    "asn",
}
ROUTE_EVIDENCE_FIELDS = {
    "schema",
    "candidate_id",
    "public_ipv4",
    "target_ipv4",
    "route_class",
    "route_contract_id",
}
EVIDENCE_BINDING_FIELDS = {
    "schema",
    "candidate_id",
    "provider_identity_evidence_sha256",
    "route_identity_evidence_sha256",
    "valid",
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


def parse_closed_key_values(
    value: bytes,
    fields: set[str],
    label: str,
) -> dict[str, str]:
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise LedgerError(f"{label} is not UTF-8") from error
    result: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), 1):
        if line.count("=") != 1:
            raise LedgerError(f"{label} line {number} is malformed")
        key, item = line.split("=", 1)
        if re.fullmatch(r"[a-z][a-z0-9_]*", key) is None or not item:
            raise LedgerError(f"{label} line {number} is malformed")
        if key in result:
            raise LedgerError(f"{label} has duplicate key: {key}")
        result[key] = item
    exact_object(result, fields, label)
    return result


def validate_resource_identity_evidence(
    candidate: dict[str, Any],
    provider_bytes: bytes,
    route_bytes: bytes,
    binding: dict[str, Any],
) -> None:
    provider = parse_closed_key_values(
        provider_bytes, PROVIDER_EVIDENCE_FIELDS, "provider evidence"
    )
    route = parse_closed_key_values(
        route_bytes, ROUTE_EVIDENCE_FIELDS, "route evidence"
    )
    exact_object(binding, EVIDENCE_BINDING_FIELDS, "resource evidence binding")
    if provider["schema"] != "knife15-m2-provider-identity-v1":
        raise LedgerError("provider evidence schema mismatch")
    if route["schema"] != "knife15-m2-route-identity-v1":
        raise LedgerError("route evidence schema mismatch")
    for field in (
        "candidate_id",
        "provider",
        "resource_id",
        "region",
        "public_ipv4",
        "asn",
    ):
        if provider[field] != str(candidate[field]):
            raise LedgerError(f"provider evidence/profile mismatch: {field}")
    for field in (
        "candidate_id",
        "public_ipv4",
        "target_ipv4",
        "route_class",
        "route_contract_id",
    ):
        if route[field] != str(candidate[field]):
            raise LedgerError(f"route evidence/profile mismatch: {field}")
    provider_sha = sha256_bytes(provider_bytes)
    route_sha = sha256_bytes(route_bytes)
    if provider_sha != candidate["provider_identity_evidence_sha256"]:
        raise LedgerError("provider evidence SHA-256 mismatch")
    if route_sha != candidate["route_identity_evidence_sha256"]:
        raise LedgerError("route evidence SHA-256 mismatch")
    expected_binding = {
        "schema": "knife15-m2-resource-evidence-binding-v1",
        "candidate_id": candidate["candidate_id"],
        "provider_identity_evidence_sha256": provider_sha,
        "route_identity_evidence_sha256": route_sha,
        "valid": True,
    }
    if binding != expected_binding:
        raise LedgerError("resource evidence binding does not match exact evidence")


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
            "evidence-binding.json",
            "direct-manifest.txt",
            "provider-identity.txt",
            "route-identity.txt",
            "exit.route.txt",
            "target.route.txt",
            "exit.traceroute.txt",
            "target.traceroute.txt",
            "remote.txt",
            "remote.stderr",
            "tuic-handshake-probe.txt",
            "tuic-handshake-probe.stderr.txt",
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
        reference = json_bytes(
            archive.bytes(f"{resource_dir}/reference-profile.json"),
            "resource reference",
        )
        exact_object(reference, set(RESOURCE_REFERENCE), "resource reference")
        if reference != RESOURCE_REFERENCE:
            raise LedgerError("resource reference is not the frozen .33 identity")
        eligibility = json_bytes(
            archive.bytes(f"{resource_dir}/eligibility.json"),
            "resource eligibility",
        )
        evidence_binding_bytes = archive.bytes(
            f"{resource_dir}/evidence-binding.json"
        )
        evidence_binding = json_bytes(
            evidence_binding_bytes,
            "resource evidence binding",
        )
        validate_resource_identity_evidence(
            candidate,
            archive.bytes(
                f"{resource_dir}/provider-identity.txt", 64 * 1024
            ),
            archive.bytes(
                f"{resource_dir}/route-identity.txt", 64 * 1024
            ),
            evidence_binding,
        )
        resource_result = parse_key_values(
            archive.text(f"{resource_dir}/result.txt"),
            "resource preflight result",
        )
        probe_bytes = archive.bytes(
            f"{resource_dir}/tuic-handshake-probe.txt", 4096
        )
        probe_lines = archive.text(
            f"{resource_dir}/tuic-handshake-probe.txt", 4096
        ).splitlines()
        probe_pattern = re.compile(
            rf"tuic_tcp_sink_probe target={re.escape(attempt['target_ipv4'])}:"
            rf"{attempt['target_iperf_port']} requested_duration_secs=1 "
            r"elapsed_ms=[0-9]+ bytes=[0-9]+ read_mbps=[0-9]+\.[0-9]{3} "
            r"reads=[0-9]+ first_rx_ms=(?:none|[0-9]+) "
            r"max_read_gap_ms=[0-9]+ eof=(?:true|false)"
        )
        matching_probe_lines = [
            line for line in probe_lines if probe_pattern.fullmatch(line) is not None
        ]
        if (
            len(matching_probe_lines) != 1
            or not probe_lines
            or probe_lines[-1] != matching_probe_lines[0]
        ):
            raise LedgerError("resource TUIC handshake probe is malformed")
        if archive.bytes(f"{resource_dir}/tuic-handshake-probe.stderr.txt"):
            raise LedgerError("resource TUIC handshake probe wrote stderr")
        if required(
            resource_result,
            "tuic_handshake_probe_sha256",
            "resource preflight result",
        ) != sha256_bytes(probe_bytes):
            raise LedgerError("resource TUIC handshake probe SHA-256 mismatch")
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
            (
                "evidence_binding_sha256",
                sha256_bytes(evidence_binding_bytes),
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


def source_runner_contract(attempt: dict[str, Any]) -> tuple[str, ...]:
    if (
        attempt["binary_sha256"] == CHECKPOINT_READER_BRIDGE_BINARY_SHA256
        and attempt["runner_sha256"] in CHECKPOINT_READER_BRIDGE_RUNNERS
    ):
        return ("checkpoint-reader-complete-tail-v1",)
    return ("exact", attempt["source_commit"], attempt["runner_sha256"])


def candidate_contract(attempt: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(
        attempt[field]
        for field in (
            "candidate_id",
            "resource_identity_sha256",
            "binary_sha256",
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
    ) + (source_runner_contract(attempt),)


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


def verify_exhaustion_certificate(
    ledger_path: Path,
    evaluation_path: Path,
    *,
    expected_ledger_sha256: str = TIER_A_EXHAUSTED_LEDGER_SHA256,
    expected_evaluation_sha256: str = TIER_A_EXHAUSTED_EVALUATION_SHA256,
) -> dict[str, Any]:
    """Verify the compact, hash-pinned Tier-A exhaustion admission pair."""

    digest(expected_ledger_sha256, "expected Tier-A ledger SHA-256")
    digest(expected_evaluation_sha256, "expected Tier-A evaluation SHA-256")
    if ledger_path.is_symlink() or not ledger_path.is_file():
        raise LedgerError("Tier-A ledger must be a regular non-symlink file")
    if evaluation_path.is_symlink() or not evaluation_path.is_file():
        raise LedgerError("Tier-A evaluation must be a regular non-symlink file")
    if ledger_path.stat().st_size > 1024 * 1024:
        raise LedgerError("Tier-A ledger exceeds the compact admission bound")
    if evaluation_path.stat().st_size > 1024 * 1024:
        raise LedgerError("Tier-A evaluation exceeds the compact admission bound")
    if sha256_file(ledger_path) != expected_ledger_sha256:
        raise LedgerError("Tier-A ledger SHA-256 does not match the reviewed exhaustion pair")
    if sha256_file(evaluation_path) != expected_evaluation_sha256:
        raise LedgerError(
            "Tier-A evaluation SHA-256 does not match the reviewed exhaustion pair"
        )

    ledger = exact_object(read_json(ledger_path), LEDGER_FIELDS, "Tier-A ledger")
    if ledger["schema"] != LEDGER_SCHEMA:
        raise LedgerError("Tier-A ledger schema mismatch")
    validate_reference(ledger["reference"])
    attempts = ledger["attempts"]
    if not isinstance(attempts, list) or not attempts:
        raise LedgerError("Tier-A exhaustion ledger has no attempts")
    validated_attempts = [
        validate_attempt(item, ledger_path.parent, verify_artifacts=False)
        for item in attempts
    ]
    if [item["sequence"] for item in validated_attempts] != list(
        range(1, len(validated_attempts) + 1)
    ):
        raise LedgerError("Tier-A exhaustion attempt sequence is not contiguous")

    evaluation = exact_object(
        read_json(evaluation_path),
        {
            "schema",
            "status",
            "accepted_candidate",
            "candidate_states",
            "ignored_invalid_mac_bundle_sha256",
            "supporting_bundle_sha256",
        },
        "Tier-A evaluation",
    )
    if evaluation["schema"] != RESULT_SCHEMA:
        raise LedgerError("Tier-A evaluation schema mismatch")
    if evaluation["status"] != "TIER_A_EXHAUSTED":
        raise LedgerError("Tier-A evaluation is not TIER_A_EXHAUSTED")
    if evaluation["accepted_candidate"] != "":
        raise LedgerError("Tier-A exhaustion evaluation names an accepted candidate")
    expected_supporting: list[str] = []
    for attempt in validated_attempts:
        if attempt["evidence_class"] == "invalid":
            continue
        expected_supporting.extend(
            (attempt["mac_bundle_sha256"], attempt["exit_bundle_sha256"])
        )
    if evaluation["supporting_bundle_sha256"] != expected_supporting:
        raise LedgerError("Tier-A evaluation supporting bundles do not match the ledger")
    rejected = [
        state
        for state in evaluation["candidate_states"]
        if isinstance(state, dict) and state.get("state") == "rejected"
    ]
    if len(rejected) != 2:
        raise LedgerError("Tier-A exhaustion does not contain exactly two rejected candidates")
    return {
        "schema": "knife15-m2-tier-a-exhaustion-admission-v1",
        "status": "PASS",
        "tier_a_status": "TIER_A_EXHAUSTED",
        "ledger_sha256": expected_ledger_sha256,
        "evaluation_sha256": expected_evaluation_sha256,
        "supporting_bundle_sha256": expected_supporting,
    }


def load_frequency_module() -> Any:
    path = Path(__file__).resolve().with_name("knife15-m2-frequency-summary.py")
    specification = importlib.util.spec_from_file_location(
        "knife15_m2_frequency_summary", path
    )
    if specification is None or specification.loader is None:
        raise LedgerError("cannot load the Tier-B frequency reducer")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


FREQUENCY = load_frequency_module()


def validate_frequency_safety(value: Any) -> dict[str, Any]:
    safety = exact_object(value, FREQUENCY_SAFETY_FIELDS, "frequency epoch safety")
    if safety["schema"] != FREQUENCY_SAFETY_SCHEMA or safety["status"] != "PASS":
        raise LedgerError("frequency epoch safety status is not exact PASS")
    for field in (
        "tcp_results",
        "udp_results",
        "dns_results",
        "real_client_results",
    ):
        positive_integer(safety[field], f"frequency safety.{field}")
    for field in (
        "tcp_max_gap_bytes",
        "invalid_result_files",
        "invalid_dns_results",
        "invalid_real_client_results",
        "process_samples",
        "network_samples",
        "endpoint_samples",
        "endpoint_conservation_max_bytes",
        "endpoint_live_bytes",
        "endpoint_outstanding_bytes",
        "interface_error_samples",
        "physical_interface_error_samples",
        "log_compactions",
        "internal_failure_matches",
    ):
        nonnegative_integer(safety[field], f"frequency safety.{field}")
    if safety["tcp_max_gap_bytes"] > 16 * 1024 * 1024:
        raise LedgerError("frequency epoch exceeds the 16MiB TCP gap gate")
    if percentage(
        safety["udp_max_loss_percent"], "frequency safety.udp_max_loss_percent"
    ) > Decimal("3.0"):
        raise LedgerError("frequency epoch exceeds the 3 percent UDP gate")
    for field in (
        "invalid_result_files",
        "invalid_dns_results",
        "invalid_real_client_results",
        "endpoint_live_bytes",
        "endpoint_outstanding_bytes",
        "interface_error_samples",
        "physical_interface_error_samples",
        "log_compactions",
        "internal_failure_matches",
    ):
        if safety[field] != 0:
            raise LedgerError(f"frequency epoch safety is nonzero: {field}")
    if safety["endpoint_conservation_max_bytes"] > 61_440:
        raise LedgerError("frequency epoch violates Endpoint conservation")
    for field in ("process_samples", "network_samples", "endpoint_samples"):
        if safety[field] < 675:
            raise LedgerError(f"frequency epoch sample coverage is too small: {field}")
    for field in (
        "resource_binding_pass",
        "observer_match",
        "network_control_pass",
        "d16_terminal_ownership_pass",
        "recovery_safety_pass",
    ):
        if boolean(safety[field], f"frequency safety.{field}") is not True:
            raise LedgerError(f"frequency epoch gate did not pass: {field}")
    return safety


def validate_raw_frequency_epoch(value: Any) -> dict[str, Any]:
    raw = exact_object(value, FREQUENCY_RAW_EPOCH_FIELDS, "raw frequency epoch")
    if raw["schema"] != FREQUENCY_RAW_EPOCH_SCHEMA:
        raise LedgerError("raw frequency epoch schema mismatch")
    token(raw["epoch_id"], "raw frequency epoch id")
    token(raw["run_id"], "raw frequency run id")
    index = nonnegative_integer(raw["run_epoch_index"], "raw run epoch index")
    if index > 3:
        raise LedgerError("one frequency run may contain at most four epochs")
    started_utc = FREQUENCY.utc(raw["started_utc"], "raw epoch start")
    ended_utc = FREQUENCY.utc(raw["ended_utc"], "raw epoch end")
    started_mono = nonnegative_integer(
        raw["started_monotonic_ns"], "raw epoch monotonic start"
    )
    ended_mono = nonnegative_integer(
        raw["ended_monotonic_ns"], "raw epoch monotonic end"
    )
    if FREQUENCY.utc_ns(ended_utc) - FREQUENCY.utc_ns(started_utc) != (
        FREQUENCY.EPOCH_NANOSECONDS
    ):
        raise LedgerError("raw frequency epoch is not exactly six UTC hours")
    if ended_mono - started_mono != FREQUENCY.EPOCH_NANOSECONDS:
        raise LedgerError("raw frequency epoch is not exactly six monotonic hours")
    FREQUENCY.validate_identity(raw["identity"])
    results = raw["tcp_results"]
    if not isinstance(results, list) or not results:
        raise LedgerError("raw frequency epoch has no TCP results")
    if len(results) > FREQUENCY.MAX_TCP_RESULTS_PER_EPOCH:
        raise LedgerError("raw frequency epoch has too many TCP results")
    expected_result_parent = PurePosixPath(
        "m2-frequency", f"epoch_{index + 1:03d}", "results"
    )
    for result in results:
        exact_object(result, FREQUENCY.TCP_RESULT_FIELDS, "frequency TCP result")
        path = PurePosixPath(result["path"])
        if path.parent != expected_result_parent:
            raise LedgerError("frequency TCP result is outside its exact epoch directory")
    validate_frequency_safety(raw["safety"])
    if raw["safety"]["tcp_results"] != len(results):
        raise LedgerError("raw frequency epoch TCP result count does not match safety")
    return raw


def build_raw_frequency_epoch(
    *,
    identity_path: Path,
    safety_path: Path,
    index_path: Path,
    evidence_root: Path,
    epoch_id: str,
    run_id: str,
    run_epoch_index: int,
    started_utc: str,
    ended_utc: str,
    started_monotonic_ns: int,
    ended_monotonic_ns: int,
) -> dict[str, Any]:
    for path, label, maximum in (
        (identity_path, "frequency identity", 64 * 1024),
        (safety_path, "frequency safety", 64 * 1024),
        (index_path, "frequency TCP index", 1024 * 1024),
    ):
        if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
            raise LedgerError(f"{label} is missing, linked, or oversized")
    identity = read_json(identity_path)
    safety = read_json(safety_path)
    lines = index_path.read_text(encoding="utf-8").splitlines()
    expected_header = (
        "result_id\tpath\treverse\tstarted_utc\tstarted_monotonic_ns"
    )
    if not lines or lines[0] != expected_header:
        raise LedgerError("frequency TCP result index header is invalid")
    results: list[dict[str, Any]] = []
    result_ids: set[str] = set()
    for number, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != 5:
            raise LedgerError(f"frequency TCP result index row {number} is malformed")
        result_id, path, reverse_text, result_utc, result_mono_text = fields
        token(result_id, f"frequency TCP result row {number} id")
        if result_id in result_ids:
            raise LedgerError("frequency TCP result id is duplicated")
        result_ids.add(result_id)
        relative = PurePosixPath(path)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise LedgerError("frequency TCP result path escapes the evidence root")
        if reverse_text not in {"0", "1"}:
            raise LedgerError("frequency TCP result reverse flag is invalid")
        FREQUENCY.utc(result_utc, f"frequency TCP result row {number} UTC")
        if re.fullmatch(r"[0-9]+", result_mono_text) is None:
            raise LedgerError("frequency TCP result monotonic timestamp is invalid")
        results.append(
            {
                "result_id": result_id,
                "path": relative.as_posix(),
                "reverse": int(reverse_text),
                "started_utc": result_utc,
                "started_monotonic_ns": int(result_mono_text),
            }
        )
    raw = {
        "schema": FREQUENCY_RAW_EPOCH_SCHEMA,
        "epoch_id": epoch_id,
        "run_id": run_id,
        "run_epoch_index": run_epoch_index,
        "started_utc": started_utc,
        "ended_utc": ended_utc,
        "started_monotonic_ns": started_monotonic_ns,
        "ended_monotonic_ns": ended_monotonic_ns,
        "identity": identity,
        "tcp_results": results,
        "safety": safety,
    }
    validate_raw_frequency_epoch(raw)
    trial_raw = copy.deepcopy(raw)
    trial_raw["run_epoch_index"] = 0
    trial_epoch = frequency_epoch_from_raw(
        trial_raw, previous=None, path_prefix="evidence"
    )
    for result in trial_epoch["tcp_results"]:
        result["path"] = result["path"].removeprefix("evidence/")
    trial_epoch["evidence_segment_index"] = 0
    trial_epoch["lifetime_index"] = 0
    summary = FREQUENCY.summarize_collection(
        {"schema": FREQUENCY.COLLECTION_SCHEMA, "epochs": [trial_epoch]},
        evidence_root=evidence_root,
    )
    if summary["status"] != "PASS":
        raise LedgerError("frequency epoch violates the Tier-B per-epoch continuity gate")
    return raw


def frequency_epoch_from_raw(
    raw: dict[str, Any],
    *,
    previous: dict[str, Any] | None,
    path_prefix: str,
) -> dict[str, Any]:
    epoch = {
        "schema": FREQUENCY.EPOCH_SCHEMA,
        "epoch_id": raw["epoch_id"],
        "evidence_segment_id": raw["run_id"],
        "evidence_segment_index": raw["run_epoch_index"],
        "lifetime_id": raw["run_id"],
        "lifetime_index": raw["run_epoch_index"],
        "gap_before": None,
        "started_utc": raw["started_utc"],
        "ended_utc": raw["ended_utc"],
        "started_monotonic_ns": raw["started_monotonic_ns"],
        "ended_monotonic_ns": raw["ended_monotonic_ns"],
        "valid": True,
        "identity": copy.deepcopy(raw["identity"]),
        "tcp_results": copy.deepcopy(raw["tcp_results"]),
    }
    for result in epoch["tcp_results"]:
        if path_prefix:
            result["path"] = f"{path_prefix}/{result['path']}"
    if previous is None:
        if raw["run_epoch_index"] != 0:
            raise LedgerError("the first sealed epoch must start a run at index zero")
        return epoch
    if raw["run_id"] == previous["run_id"]:
        if raw["run_epoch_index"] != previous["run_epoch_index"] + 1:
            raise LedgerError("a frequency run is missing a sealed epoch")
        return epoch
    if raw["run_epoch_index"] != 0:
        raise LedgerError("a new frequency run must start at epoch index zero")
    previous_end_utc = FREQUENCY.utc(previous["ended_utc"], "previous epoch end")
    current_start_utc = FREQUENCY.utc(raw["started_utc"], "current epoch start")
    previous_end_mono = previous["ended_monotonic_ns"]
    current_start_mono = raw["started_monotonic_ns"]
    if current_start_utc <= previous_end_utc or current_start_mono <= previous_end_mono:
        raise LedgerError("a new frequency run must leave an explicit positive evidence gap")
    epoch["gap_before"] = {
        "reason": "evidence-unavailable",
        "started_utc": previous["ended_utc"],
        "ended_utc": raw["started_utc"],
        "started_monotonic_ns": previous_end_mono,
        "ended_monotonic_ns": current_start_mono,
    }
    return epoch


def summarize_raw_frequency_run(
    raw_epochs: list[Any], evidence_root: Path
) -> dict[str, Any]:
    if not raw_epochs or len(raw_epochs) > 4:
        raise LedgerError("a raw frequency run requires one to four epochs")
    epochs: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for item in raw_epochs:
        raw = validate_raw_frequency_epoch(item)
        if previous is not None and raw["run_id"] != previous["run_id"]:
            raise LedgerError("one raw frequency run changed run identity")
        epochs.append(
            frequency_epoch_from_raw(raw, previous=previous, path_prefix="")
        )
        previous = raw
    return FREQUENCY.summarize_collection(
        {"schema": FREQUENCY.COLLECTION_SCHEMA, "epochs": epochs},
        evidence_root=evidence_root,
    )


def validate_frequency_record(
    value: Any,
    artifact_root: Path,
    *,
    verify_artifacts: bool,
) -> dict[str, Any]:
    record = exact_object(value, FREQUENCY_RECORD_FIELDS, "frequency epoch record")
    if record["schema"] != FREQUENCY_RECORD_SCHEMA:
        raise LedgerError("frequency epoch record schema mismatch")
    positive_integer(record["sequence"], "frequency epoch sequence", 12)
    raw = validate_raw_frequency_epoch(record["raw_epoch"])
    digest(record["raw_epoch_sha256"], "raw frequency epoch SHA-256")
    if canonical_sha256(raw) != record["raw_epoch_sha256"]:
        raise LedgerError("raw frequency epoch SHA-256 mismatch")
    mac_name = bundle_name(record["mac_bundle_name"], "frequency Mac bundle")
    exit_name = bundle_name(record["exit_bundle_name"], "frequency Exit bundle")
    digest(record["mac_bundle_sha256"], "frequency Mac bundle SHA-256")
    digest(record["exit_bundle_sha256"], "frequency Exit bundle SHA-256")
    if verify_artifacts:
        verify_bundle(
            artifact_root,
            mac_name,
            record["mac_bundle_sha256"],
            "frequency Mac bundle",
        )
        verify_bundle(
            artifact_root,
            exit_name,
            record["exit_bundle_sha256"],
            "frequency Exit bundle",
        )
    return record


def validate_and_materialize_frequency_record(
    record: dict[str, Any], artifact_root: Path, materialized_root: Path
) -> None:
    raw = record["raw_epoch"]
    identity = raw["identity"]
    archive = EvidenceArchive(
        artifact_root / record["mac_bundle_name"], "frequency Mac bundle"
    )
    epoch_directory = f"m2-frequency/epoch_{raw['run_epoch_index'] + 1:03d}"
    try:
        archived_raw = json_bytes(
            archive.bytes(f"{epoch_directory}/epoch.json"),
            "archived frequency epoch",
        )
        if archived_raw != raw:
            raise LedgerError("sealed frequency epoch does not match its Mac bundle")
        manifest = parse_key_values(archive.text("manifest.txt"), "Mac manifest")
        summary = parse_summary(archive.text("summary.md"))
        resource_directory = "m2-frequency-resource-preflight"
        candidate = json_bytes(
            archive.bytes(f"{resource_directory}/candidate-profile.json"),
            "frequency resource profile",
        )
        exact_object(candidate, RESOURCE_PROFILE_FIELDS, "frequency resource profile")
        if candidate.get("schema") != "knife15-m2-resource-profile-v1":
            raise LedgerError("frequency resource profile schema mismatch")
        eligibility = json_bytes(
            archive.bytes(f"{resource_directory}/eligibility.json"),
            "frequency resource eligibility",
        )
        binding = json_bytes(
            archive.bytes(f"{resource_directory}/evidence-binding.json"),
            "frequency resource evidence binding",
        )
        validate_resource_identity_evidence(
            candidate,
            archive.bytes(f"{resource_directory}/provider-identity.txt"),
            archive.bytes(f"{resource_directory}/route-identity.txt"),
            binding,
        )
        validate_resource_archive(archive, "m2-frequency")
        if eligibility.get("eligible") is not True or eligibility.get(
            "candidate_profile_sha256"
        ) != canonical_sha256(candidate):
            raise LedgerError("frequency resource eligibility/profile mismatch")
        resource_result = parse_key_values(
            archive.text(f"{resource_directory}/result.txt"),
            "frequency resource preflight result",
        )
        for key, expected in (
            ("status", "pass"),
            ("candidate_id", identity["candidate_id"]),
            ("candidate_ipv4", identity["exit_ipv4"]),
            ("candidate_tuic_port", str(identity["tuic_port"])),
            ("target", identity["target_ipv4"]),
            ("target_iperf_port", str(identity["target_iperf_port"])),
            ("source_commit", identity["source_commit"]),
            ("binary_sha256", identity["binary_sha256"]),
            ("observer_sha256", identity["observer_sha256"]),
        ):
            if required(resource_result, key, "frequency resource preflight") != expected:
                raise LedgerError(f"frequency resource preflight mismatch: {key}")
        admission = json_bytes(
            archive.bytes("m2-frequency-tier-a-admission.json"),
            "frequency Tier-A admission",
        )
        expected_admission = {
            "schema": "knife15-m2-tier-a-exhaustion-admission-v1",
            "status": "PASS",
            "tier_a_status": "TIER_A_EXHAUSTED",
            "ledger_sha256": TIER_A_EXHAUSTED_LEDGER_SHA256,
            "evaluation_sha256": TIER_A_EXHAUSTED_EVALUATION_SHA256,
        }
        for key, expected in expected_admission.items():
            if admission.get(key) != expected:
                raise LedgerError(f"frequency Tier-A admission mismatch: {key}")
        if archive.text("m2-frequency-resource-admission.status", 64).splitlines() != [
            "pass"
        ]:
            raise LedgerError("frequency resource admission is not exact PASS")
        if archive.text("m2-frequency.status", 64).splitlines() not in (
            ["complete"],
            ["failed"],
            ["interrupted"],
        ):
            raise LedgerError("frequency parent run has no terminal status")
        for key, expected in (
            ("source_commit", identity["source_commit"]),
            ("binary_sha256", identity["binary_sha256"]),
            ("runner_sha256", identity["runner_sha256"]),
            ("exit_host", identity["exit_ipv4"]),
            ("exit_port", str(identity["tuic_port"])),
            ("target", identity["target_ipv4"]),
            ("iperf_port", str(identity["target_iperf_port"])),
        ):
            if required(manifest, key, "Mac manifest") != expected:
                raise LedgerError(f"frequency Mac manifest mismatch: {key}")
        for key, expected in (
            ("candidate_id", identity["candidate_id"]),
            ("source_commit", identity["source_commit"]),
            ("client_binary_sha256", identity["binary_sha256"]),
            ("server_binary_sha256", identity["server_binary_sha256"]),
            ("server_config_sha256", identity["server_config_sha256"]),
            ("observer_sha256", identity["observer_sha256"]),
            ("public_ipv4", identity["exit_ipv4"]),
            ("tuic_port", identity["tuic_port"]),
            ("target_ipv4", identity["target_ipv4"]),
            ("target_iperf_port", identity["target_iperf_port"]),
        ):
            if candidate.get(key) != expected:
                raise LedgerError(f"frequency resource profile mismatch: {key}")
        if resource_identity(candidate) != identity["resource_profile_sha256"]:
            raise LedgerError("frequency stable resource identity SHA-256 mismatch")
        contract = archive.bytes("m2-frequency-workload-contract.txt", 64 * 1024)
        if sha256_bytes(contract) != identity["workload_contract_sha256"]:
            raise LedgerError("frequency workload contract SHA-256 mismatch")
        contract_values = parse_key_values(
            contract.decode("utf-8"), "frequency workload contract"
        )
        for key, expected in (
            ("schema", "knife15-m2-frequency-workload-v1"),
            ("epoch_secs", "21600"),
            ("active_planned_secs", "20700"),
            ("boundary_reserve_secs", "900"),
            ("mode_order", "steady,quiet,churn,steady"),
            ("max_epochs_per_run", "4"),
            ("minimum_process_network_endpoint_samples_per_epoch", "675"),
            ("udp_loss_limit_percent", "3.0"),
            ("tcp_gap_limit_bytes", "16777216"),
            ("target", identity["target_ipv4"]),
            ("iperf_port", str(identity["target_iperf_port"])),
        ):
            if required(contract_values, key, "frequency workload contract") != expected:
                raise LedgerError(f"frequency workload contract mismatch: {key}")
        for key in ("baseline_forward_sha256", "baseline_reverse_sha256"):
            digest(
                required(contract_values, key, "frequency workload contract"),
                f"frequency workload contract.{key}",
            )
        observer_status = parse_key_values(
            archive.text(f"{epoch_directory}/observer-status.txt"),
            "frequency epoch observer status",
        )
        for key, expected in (
            ("status", "active"),
            ("observer_healthy", "1"),
            ("target", identity["target_ipv4"]),
            ("iperf_port", str(identity["target_iperf_port"])),
            ("tuic_port", str(identity["tuic_port"])),
        ):
            if required(observer_status, key, "frequency epoch observer status") != expected:
                raise LedgerError(f"frequency epoch observer mismatch: {key}")
        finalization = parse_key_values(
            archive.text("m2-exit-observer-finalization.txt"),
            "frequency observer finalization",
        )
        for key, expected in (
            ("observer_script_sha256", identity["observer_sha256"]),
            ("sha256", record["exit_bundle_sha256"]),
            ("finalization_status", "complete"),
        ):
            if required(finalization, key, "frequency observer finalization") != expected:
                raise LedgerError(f"frequency observer finalization mismatch: {key}")
        for key, expected in (
            ("cleanup_evidence", "PASS"),
            ("stop_cleanup_complete", "1"),
            ("endpoint_conservation", "PASS"),
            ("recovery_evidence_safety", "PASS"),
            ("interface_error_samples", "0"),
            ("physical_interface_error_samples", "0"),
        ):
            if required(summary, key, "frequency Mac summary") != expected:
                raise LedgerError(f"frequency Mac final safety mismatch: {key}")
        secret = archive.text("secret-scan.txt", 4096).splitlines()
        if len(secret) != 1 or not secret[0].startswith("PASS:"):
            raise LedgerError("frequency Mac secret scan did not pass")
        events = archive.text("events.tsv")
        sealed_pattern = (
            rf"^[^\t]+\tm2-frequency epoch sealed epoch_id="
            rf"{re.escape(raw['epoch_id'])} index={raw['run_epoch_index']} "
            rf"ended_utc={re.escape(raw['ended_utc'])} "
            rf"ended_monotonic_ns={raw['ended_monotonic_ns']}$"
        )
        if len(re.findall(sealed_pattern, events, re.M)) != 1:
            raise LedgerError("frequency epoch has no exact seal event")
        destination_prefix = materialized_root / record["mac_bundle_sha256"]
        for result in raw["tcp_results"]:
            relative = PurePosixPath(result["path"])
            data = archive.bytes(relative.as_posix())
            destination = destination_prefix.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if destination.read_bytes() != data:
                    raise LedgerError("frequency result path changed within one Mac bundle")
            else:
                destination.write_bytes(data)
    finally:
        archive.close()
    validate_exit_bundle(
        {
            "exit_bundle_name": record["exit_bundle_name"],
            "target_ipv4": identity["target_ipv4"],
            "target_iperf_port": identity["target_iperf_port"],
            "tuic_port": identity["tuic_port"],
            "started_utc": raw["started_utc"],
            "ended_utc": raw["ended_utc"],
        },
        artifact_root,
    )


def validate_frequency_source_tree(record: dict[str, Any], repo: Path) -> None:
    source = record["raw_epoch"]["identity"]["source_commit"]
    floor = git_command(repo, ["rev-parse", f"{FREQUENCY_SOURCE_FLOOR}^{{commit}}"])
    if floor.returncode != 0:
        raise LedgerError("frequency source floor is unavailable")
    source_commit = git_command(repo, ["rev-parse", f"{source}^{{commit}}"])
    if source_commit.returncode != 0:
        raise LedgerError("frequency source commit is unavailable")
    ancestor = git_command(
        repo,
        ["merge-base", "--is-ancestor", floor.stdout.decode().strip(), source],
    )
    if ancestor.returncode != 0:
        raise LedgerError("frequency source predates its reviewed source floor")
    runner = git_command(repo, ["show", f"{source}:scripts/knife15-macos-soak.sh"])
    if runner.returncode != 0:
        raise LedgerError("cannot read the frequency runner at the tested source")
    if sha256_bytes(runner.stdout) != record["raw_epoch"]["identity"]["runner_sha256"]:
        raise LedgerError("frequency runner hash does not match the tested source")
    for relative in (
        "scripts/knife15-m2-continuity-ledger.py",
        "scripts/knife15-m2-frequency-summary.py",
    ):
        blob = git_command(repo, ["show", f"{source}:{relative}"])
        if blob.returncode != 0:
            raise LedgerError(f"frequency source is missing {relative}")


def derive_frequency_record(
    *,
    artifact_root: Path,
    repo: Path,
    sequence: int,
    run_epoch_index: int,
    mac_bundle_name: str,
    mac_bundle_sha256: str,
    exit_bundle_name: str,
    exit_bundle_sha256: str,
) -> dict[str, Any]:
    positive_integer(sequence, "frequency epoch sequence", 12)
    nonnegative_integer(run_epoch_index, "frequency run epoch index")
    if run_epoch_index > 3:
        raise LedgerError("frequency run epoch index exceeds three")
    bundle_name(mac_bundle_name, "frequency Mac bundle")
    bundle_name(exit_bundle_name, "frequency Exit bundle")
    digest(mac_bundle_sha256, "frequency Mac bundle SHA-256")
    digest(exit_bundle_sha256, "frequency Exit bundle SHA-256")
    verify_bundle(artifact_root, mac_bundle_name, mac_bundle_sha256, "frequency Mac bundle")
    verify_bundle(artifact_root, exit_bundle_name, exit_bundle_sha256, "frequency Exit bundle")
    archive = EvidenceArchive(artifact_root / mac_bundle_name, "frequency Mac bundle")
    try:
        raw = json_bytes(
            archive.bytes(
                f"m2-frequency/epoch_{run_epoch_index + 1:03d}/epoch.json"
            ),
            "sealed frequency epoch",
        )
    finally:
        archive.close()
    record = {
        "schema": FREQUENCY_RECORD_SCHEMA,
        "sequence": sequence,
        "raw_epoch": raw,
        "raw_epoch_sha256": canonical_sha256(raw),
        "mac_bundle_name": mac_bundle_name,
        "mac_bundle_sha256": mac_bundle_sha256,
        "exit_bundle_name": exit_bundle_name,
        "exit_bundle_sha256": exit_bundle_sha256,
    }
    validate_frequency_record(record, artifact_root, verify_artifacts=True)
    with tempfile.TemporaryDirectory(prefix="knife15-frequency-seal-") as tmp:
        validate_and_materialize_frequency_record(record, artifact_root, Path(tmp))
    validate_frequency_source_tree(record, repo)
    return record


def evaluate_frequency_ledger(
    value: Any,
    evidence_root: Path,
    *,
    verify_artifacts: bool = False,
) -> dict[str, Any]:
    ledger = exact_object(value, FREQUENCY_LEDGER_FIELDS, "frequency ledger")
    if ledger["schema"] != FREQUENCY_LEDGER_SCHEMA:
        raise LedgerError("frequency ledger schema mismatch")
    if ledger["tier_a_ledger_sha256"] != TIER_A_EXHAUSTED_LEDGER_SHA256:
        raise LedgerError("frequency ledger is not bound to the reviewed Tier-A ledger")
    if ledger["tier_a_evaluation_sha256"] != TIER_A_EXHAUSTED_EVALUATION_SHA256:
        raise LedgerError("frequency ledger is not bound to Tier-A exhaustion")
    raw_records = ledger["epochs"]
    if not isinstance(raw_records, list) or not raw_records:
        raise LedgerError("frequency ledger must contain at least one sealed epoch")
    if len(raw_records) > 12:
        raise LedgerError("frequency ledger exceeds twelve sealed epochs")
    records = [
        validate_frequency_record(item, evidence_root, verify_artifacts=verify_artifacts)
        for item in raw_records
    ]
    materialized: tempfile.TemporaryDirectory[str] | None = None
    summary_root = evidence_root
    if verify_artifacts:
        materialized = tempfile.TemporaryDirectory(prefix="knife15-m2-frequency-ledger-")
        summary_root = Path(materialized.name)
        for record in records:
            validate_and_materialize_frequency_record(
                record, evidence_root, summary_root
            )
    if [record["sequence"] for record in records] != list(range(1, len(records) + 1)):
        raise LedgerError("frequency epoch sequence must be contiguous from one")
    identity = records[0]["raw_epoch"]["identity"]
    if any(record["raw_epoch"]["identity"] != identity for record in records[1:]):
        raise LedgerError("frequency epochs changed immutable identity")
    epoch_ids = [record["raw_epoch"]["epoch_id"] for record in records]
    if len(epoch_ids) != len(set(epoch_ids)):
        raise LedgerError("frequency epoch id was reused")
    bundle_pairs: dict[str, str] = {}
    reverse_bundle_pairs: dict[str, str] = {}
    epochs: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for record in records:
        mac_sha = record["mac_bundle_sha256"]
        exit_sha = record["exit_bundle_sha256"]
        prior_exit = bundle_pairs.setdefault(mac_sha, exit_sha)
        if prior_exit != exit_sha:
            raise LedgerError("one frequency Mac bundle names multiple Exit bundles")
        prior_mac = reverse_bundle_pairs.setdefault(exit_sha, mac_sha)
        if prior_mac != mac_sha:
            raise LedgerError("one frequency Exit bundle names multiple Mac runs")
        raw = record["raw_epoch"]
        epochs.append(
            frequency_epoch_from_raw(
                raw,
                previous=previous,
                path_prefix=mac_sha,
            )
        )
        previous = raw
    summary = FREQUENCY.summarize_collection(
        {"schema": FREQUENCY.COLLECTION_SCHEMA, "epochs": epochs},
        evidence_root=summary_root,
    )
    if materialized is not None:
        materialized.cleanup()
    violations = list(summary["violations"])
    if len(records) == 12 and summary["max_contiguous_valid_epochs_same_lifetime"] < 4:
        violations.append("uninterrupted_four_epoch_lifetime")
    if summary["status"] == "FAIL" or violations:
        status = "TIER_B_FAILED"
    elif len(records) == 12:
        status = "TIER_B_ACCEPTED"
    else:
        status = "TIER_B_PENDING"
    return {
        "schema": FREQUENCY_LEDGER_RESULT_SCHEMA,
        "status": status,
        "valid_epochs": summary["valid_epochs"],
        "valid_hours": summary["valid_hours"],
        "identity_sha256": summary["identity_sha256"],
        "receiver_zero_episodes": summary["receiver_zero_episodes"],
        "receiver_zero_intervals": summary["receiver_zero_intervals"],
        "max_consecutive_receiver_zero_intervals": summary[
            "max_consecutive_receiver_zero_intervals"
        ],
        "max_receiver_zero_episodes_rolling_6h": summary[
            "max_receiver_zero_episodes_rolling_6h"
        ],
        "max_receiver_zero_episodes_rolling_24h": summary[
            "max_receiver_zero_episodes_rolling_24h"
        ],
        "max_receiver_zero_intervals_rolling_24h": summary[
            "max_receiver_zero_intervals_rolling_24h"
        ],
        "max_contiguous_valid_epochs_same_lifetime": summary[
            "max_contiguous_valid_epochs_same_lifetime"
        ],
        "violations": violations,
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
    provider_evidence = key_value_bytes(
        {
            "schema": "knife15-m2-provider-identity-v1",
            "candidate_id": candidate_id,
            "provider": "provider-parser",
            "resource_id": "resource-parser",
            "region": "us-parser",
            "public_ipv4": "1.1.1.1",
            "asn": 13335,
        }
    )
    route_evidence = key_value_bytes(
        {
            "schema": "knife15-m2-route-identity-v1",
            "candidate_id": candidate_id,
            "public_ipv4": "1.1.1.1",
            "target_ipv4": "43.130.32.77",
            "route_class": "premium-parser",
            "route_contract_id": "contract-parser",
        }
    )
    provider_evidence_sha = sha256_bytes(provider_evidence)
    route_evidence_sha = sha256_bytes(route_evidence)
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
        "provider_identity_evidence_sha256": provider_evidence_sha,
        "route_identity_evidence_sha256": route_evidence_sha,
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
    evidence_binding = {
        "schema": "knife15-m2-resource-evidence-binding-v1",
        "candidate_id": candidate_id,
        "provider_identity_evidence_sha256": provider_evidence_sha,
        "route_identity_evidence_sha256": route_evidence_sha,
        "valid": True,
    }
    tuic_handshake_probe = (
        b"fixture QUIC startup diagnostic\n"
        b"tuic_tcp_sink_probe target=43.130.32.77:5201 "
        b"requested_duration_secs=1 elapsed_ms=1000 bytes=0 "
        b"read_mbps=0.000 reads=0 first_rx_ms=none "
        b"max_read_gap_ms=0 eof=false\n"
    )
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
        "evidence_binding_sha256": sha256_bytes(
            (canonical_json(evidence_binding) + "\n").encode()
        ),
        "tuic_handshake_probe_sha256": sha256_bytes(tuic_handshake_probe),
    }
    resource_files: dict[str, bytes] = {
        "reference-profile.json": (
            canonical_json(RESOURCE_REFERENCE) + "\n"
        ).encode(),
        "candidate-profile.json": (canonical_json(candidate) + "\n").encode(),
        "eligibility.json": (canonical_json(eligibility) + "\n").encode(),
        "evidence-binding.json": (
            canonical_json(evidence_binding) + "\n"
        ).encode(),
        "direct-manifest.txt": direct_manifest,
        "provider-identity.txt": provider_evidence,
        "route-identity.txt": route_evidence,
        "exit.route.txt": b"interface: en0\n",
        "target.route.txt": b"interface: en0\n",
        "exit.traceroute.txt": b"exit traceroute\n",
        "target.traceroute.txt": b"target traceroute\n",
        "remote.txt": b"schema=knife15-m2-resource-remote-v1\n",
        "remote.stderr": b"",
        "tuic-handshake-probe.txt": tuic_handshake_probe,
        "tuic-handshake-probe.stderr.txt": b"",
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


def make_valid_frequency_record(artifact_root: Path) -> dict[str, Any]:
    """Build one fully replayable interrupted-parent frequency epoch fixture."""
    base_attempt = make_valid_evidence_attempt(artifact_root, "formal")
    base_archive = EvidenceArchive(
        artifact_root / base_attempt["mac_bundle_name"],
        "frequency fixture base Mac bundle",
    )
    try:
        candidate = json_bytes(
            base_archive.bytes("m2-resource-preflight/candidate-profile.json"),
            "frequency fixture candidate",
        )
        resource_members = {
            name.removeprefix("m2-resource-preflight/"): base_archive.bytes(name)
            for name in base_archive.files
            if name.startswith("m2-resource-preflight/")
        }
        resource_archive = base_archive.bytes(
            "m2-resource-preflight.tar.gz", 32 * 1024 * 1024
        )
        resource_archive_sha = base_archive.bytes(
            "m2-resource-preflight.tar.gz.sha256", 1024
        )
    finally:
        base_archive.close()

    contract = key_value_bytes(
        {
            "schema": "knife15-m2-frequency-workload-v1",
            "epoch_secs": 21600,
            "active_planned_secs": 20700,
            "boundary_reserve_secs": 900,
            "mode_order": "steady,quiet,churn,steady",
            "max_epochs_per_run": 4,
            "minimum_process_network_endpoint_samples_per_epoch": 675,
            "udp_loss_limit_percent": "3.0",
            "tcp_gap_limit_bytes": 16777216,
            "baseline_forward_sha256": "7" * 64,
            "baseline_reverse_sha256": "8" * 64,
            "baseline_forward_bps": 20_000_000,
            "baseline_reverse_bps": 40_000_000,
            "target": candidate["target_ipv4"],
            "iperf_port": candidate["target_iperf_port"],
        }
    )
    identity = {
        "candidate_id": candidate["candidate_id"],
        "source_commit": base_attempt["source_commit"],
        "binary_sha256": base_attempt["binary_sha256"],
        "runner_sha256": base_attempt["runner_sha256"],
        "resource_profile_sha256": resource_identity(candidate),
        "workload_contract_sha256": sha256_bytes(contract),
        "server_binary_sha256": candidate["server_binary_sha256"],
        "server_config_sha256": candidate["server_config_sha256"],
        "observer_sha256": candidate["observer_sha256"],
        "exit_ipv4": candidate["public_ipv4"],
        "target_ipv4": candidate["target_ipv4"],
        "tuic_port": candidate["tuic_port"],
        "target_iperf_port": candidate["target_iperf_port"],
    }
    generated = FREQUENCY.generated_collection(
        {
            "schema": FREQUENCY.GENERATED_SCENARIO_SCHEMA,
            "epoch_count": 1,
            "events": [],
            "mutation": "none",
            "expect": {},
        }
    )["epochs"][0]
    raw = {
        "schema": FREQUENCY_RAW_EPOCH_SCHEMA,
        "epoch_id": "frequency-parser-run-1",
        "run_id": "frequency-parser-run",
        "run_epoch_index": 0,
        "started_utc": generated["started_utc"],
        "ended_utc": generated["ended_utc"],
        "started_monotonic_ns": generated["started_monotonic_ns"],
        "ended_monotonic_ns": generated["ended_monotonic_ns"],
        "identity": identity,
        "tcp_results": copy.deepcopy(generated["tcp_results"]),
        "safety": {
            "schema": FREQUENCY_SAFETY_SCHEMA,
            "status": "PASS",
            "tcp_results": len(generated["tcp_results"]),
            "udp_results": 1,
            "tcp_max_gap_bytes": 0,
            "udp_max_loss_percent": "0.000000",
            "invalid_result_files": 0,
            "dns_results": 1,
            "invalid_dns_results": 0,
            "real_client_results": 1,
            "invalid_real_client_results": 0,
            "process_samples": 720,
            "network_samples": 720,
            "endpoint_samples": 720,
            "endpoint_conservation_max_bytes": 61_440,
            "endpoint_live_bytes": 0,
            "endpoint_outstanding_bytes": 0,
            "resource_binding_pass": True,
            "observer_match": True,
            "network_control_pass": True,
            "d16_terminal_ownership_pass": True,
            "recovery_safety_pass": True,
            "interface_error_samples": 0,
            "physical_interface_error_samples": 0,
            "log_compactions": 0,
            "internal_failure_matches": 0,
        },
    }
    for result in raw["tcp_results"]:
        result["path"] = f"m2-frequency/epoch_001/{result['path']}"
    validate_raw_frequency_epoch(raw)

    exit_name = "mini_vpn_knife15_exit_frequency_parser.tar.gz"
    exit_path = artifact_root / exit_name
    exit_files: dict[str, bytes] = {
        "metadata.txt": key_value_bytes(
            {
                "schema": "knife15-exit-target-observer-v2",
                "started_at": raw["started_utc"],
                "target": identity["target_ipv4"],
                "iperf_port": identity["target_iperf_port"],
                "tuic_port": identity["tuic_port"],
                "stopped_at": raw["ended_utc"],
                "frozen_at": raw["ended_utc"],
            }
        ),
        "counters.csv": (
            b"timestamp,epoch,target_ingress_packets,target_ingress_bytes,"
            b"target_egress_packets,target_egress_bytes,tuic_ingress_packets,"
            b"tuic_ingress_bytes,tuic_egress_packets,tuic_egress_bytes\n"
            + f"{raw['started_utc']},1,1,100,1,100,1,100,1,100\n".encode()
            + f"{raw['ended_utc']},2,2,200,2,200,2,200,2,200\n".encode()
        ),
        "tcpdump.stderr": b"10 packets captured\n0 packets dropped by kernel\n",
        "secret-scan.txt": b"PASS: no credential names found\n",
        "capture.pcap00": b"frequency fixture capture",
    }
    exit_files["SHA256SUMS"] = "".join(
        f"{sha256_bytes(value)}  ./{name}\n"
        for name, value in sorted(exit_files.items())
    ).encode()
    write_test_archive(exit_path, "mini_vpn_knife15_exit_frequency_parser", exit_files)
    exit_sha = sha256_file(exit_path)

    admission = {
        "schema": "knife15-m2-tier-a-exhaustion-admission-v1",
        "status": "PASS",
        "tier_a_status": "TIER_A_EXHAUSTED",
        "ledger_sha256": TIER_A_EXHAUSTED_LEDGER_SHA256,
        "evaluation_sha256": TIER_A_EXHAUSTED_EVALUATION_SHA256,
        "supporting_bundle_sha256": [],
    }
    epoch_directory = "m2-frequency/epoch_001"
    summary = "# frequency fixture\n\n" + "".join(
        f"- {key}: {value}\n"
        for key, value in {
            "cleanup_evidence": "PASS",
            "stop_cleanup_complete": "1",
            "endpoint_conservation": "PASS",
            "recovery_evidence_safety": "PASS",
            "interface_error_samples": "0",
            "physical_interface_error_samples": "0",
        }.items()
    )
    mac_files: dict[str, bytes] = {
        "manifest.txt": key_value_bytes(
            {
                "source_commit": identity["source_commit"],
                "binary_sha256": identity["binary_sha256"],
                "runner_sha256": identity["runner_sha256"],
                "exit_host": identity["exit_ipv4"],
                "exit_port": identity["tuic_port"],
                "target": identity["target_ipv4"],
                "iperf_port": identity["target_iperf_port"],
            }
        ),
        "summary.md": summary.encode(),
        "secret-scan.txt": b"PASS: no credential names found\n",
        "m2-frequency.status": b"interrupted\n",
        "m2-frequency-resource-admission.status": b"pass\n",
        "m2-frequency-tier-a-admission.json": (
            canonical_json(admission) + "\n"
        ).encode(),
        "m2-frequency-workload-contract.txt": contract,
        "m2-frequency-resource-preflight/candidate-profile.json": (
            canonical_json(candidate) + "\n"
        ).encode(),
        "m2-frequency-resource-preflight.tar.gz": resource_archive,
        "m2-frequency-resource-preflight.tar.gz.sha256": resource_archive_sha,
        f"{epoch_directory}/epoch.json": (canonical_json(raw) + "\n").encode(),
        f"{epoch_directory}/observer-status.txt": key_value_bytes(
            {
                "status": "active",
                "observer_healthy": 1,
                "target": identity["target_ipv4"],
                "iperf_port": identity["target_iperf_port"],
                "tuic_port": identity["tuic_port"],
            }
        ),
        "m2-exit-observer-finalization.txt": key_value_bytes(
            {
                "observer_script_sha256": identity["observer_sha256"],
                "sha256": exit_sha,
                "finalization_status": "complete",
            }
        ),
        "events.tsv": (
            "timestamp\tevent\n"
            f"{raw['ended_utc']}\tm2-frequency epoch sealed "
            f"epoch_id={raw['epoch_id']} index=0 "
            f"ended_utc={raw['ended_utc']} "
            f"ended_monotonic_ns={raw['ended_monotonic_ns']}\n"
        ).encode(),
    }
    for name, value in resource_members.items():
        mac_files[f"m2-frequency-resource-preflight/{name}"] = value
    fixture_root = Path(__file__).resolve().parent / "fixtures" / (
        "knife15-m2-frequency"
    )
    for result in raw["tcp_results"]:
        source_path = PurePosixPath(result["path"])
        fixture_relative = PurePosixPath(*source_path.parts[2:])
        mac_files[result["path"]] = (fixture_root / fixture_relative).read_bytes()
    mac_name = "mini_vpn_knife15_macos_frequency_parser.tar.gz"
    mac_path = artifact_root / mac_name
    write_test_archive(mac_path, "mini_vpn_knife15_macos_frequency_parser", mac_files)
    return {
        "schema": FREQUENCY_RECORD_SCHEMA,
        "sequence": 1,
        "raw_epoch": raw,
        "raw_epoch_sha256": canonical_sha256(raw),
        "mac_bundle_name": mac_name,
        "mac_bundle_sha256": sha256_file(mac_path),
        "exit_bundle_name": exit_name,
        "exit_bundle_sha256": exit_sha,
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
        resource_fixture = read_json(
            Path(__file__).resolve().parent
            / "fixtures"
            / "knife15-m2-resource"
            / "distinct-provider.json"
        )
        changed_interface = json.loads(canonical_json(resource_fixture))
        changed_interface["mac_interface"] = "en5"
        if resource_identity(resource_fixture) == resource_identity(changed_interface):
            raise AssertionError("Mac physical-interface drift kept one resource identity")
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
        qualification_archive = EvidenceArchive(
            artifact_root / qualification_attempt["mac_bundle_name"],
            "qualification identity fixture",
        )
        try:
            resource_prefix = "m2-qualification-resource-preflight"
            fixture_candidate = json_bytes(
                qualification_archive.bytes(
                    f"{resource_prefix}/candidate-profile.json"
                ),
                "fixture candidate",
            )
            fixture_provider = qualification_archive.bytes(
                f"{resource_prefix}/provider-identity.txt"
            )
            fixture_route = qualification_archive.bytes(
                f"{resource_prefix}/route-identity.txt"
            )
            fixture_binding = json_bytes(
                qualification_archive.bytes(
                    f"{resource_prefix}/evidence-binding.json"
                ),
                "fixture binding",
            )
            refreshed_direct = copy.deepcopy(fixture_candidate)
            refreshed_direct["workload_profile_sha256"] = "9" * 64
            if resource_identity(refreshed_direct) != resource_identity(
                fixture_candidate
            ):
                raise AssertionError(
                    "fresh direct evidence changed the stable frequency resource identity"
                )
            changed_server = copy.deepcopy(fixture_candidate)
            changed_server["server_config_sha256"] = "9" * 64
            if resource_identity(changed_server) == resource_identity(
                fixture_candidate
            ):
                raise AssertionError(
                    "server configuration drift kept one frequency resource identity"
                )
            validate_resource_identity_evidence(
                fixture_candidate,
                fixture_provider,
                fixture_route,
                fixture_binding,
            )
            try:
                validate_resource_identity_evidence(
                    fixture_candidate,
                    fixture_provider.replace(
                        b"provider=provider-parser\n",
                        b"provider=wrong-provider\n",
                    ),
                    fixture_route,
                    fixture_binding,
                )
            except LedgerError:
                pass
            else:
                raise AssertionError(
                    "ledger accepted provider evidence/profile mismatch"
                )
        finally:
            qualification_archive.close()
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
        exhaustion_ledger_path = artifact_root / "exhaustion-ledger.json"
        exhaustion_evaluation_path = artifact_root / "exhaustion-evaluation.json"
        exhaustion_ledger_path.write_text(
            canonical_json(ledgers["two-rejected-candidates.json"]) + "\n",
            encoding="utf-8",
        )
        exhaustion_evaluation_path.write_text(
            canonical_json(exhausted) + "\n",
            encoding="utf-8",
        )
        admission = verify_exhaustion_certificate(
            exhaustion_ledger_path,
            exhaustion_evaluation_path,
            expected_ledger_sha256=sha256_file(exhaustion_ledger_path),
            expected_evaluation_sha256=sha256_file(exhaustion_evaluation_path),
        )
        assert admission["tier_a_status"] == "TIER_A_EXHAUSTED"
        pending_evaluation = json.loads(canonical_json(exhausted))
        pending_evaluation["status"] = "TIER_A_PENDING"
        exhaustion_evaluation_path.write_text(
            canonical_json(pending_evaluation) + "\n",
            encoding="utf-8",
        )
        try:
            verify_exhaustion_certificate(
                exhaustion_ledger_path,
                exhaustion_evaluation_path,
                expected_ledger_sha256=sha256_file(exhaustion_ledger_path),
                expected_evaluation_sha256=sha256_file(exhaustion_evaluation_path),
            )
        except LedgerError:
            pass
        else:
            raise AssertionError("Tier-B admission accepted a pending Tier-A evaluation")

        frequency_scenario = {
            "schema": FREQUENCY.GENERATED_SCENARIO_SCHEMA,
            "epoch_count": 12,
            "events": [],
            "mutation": "none",
            "expect": {},
        }
        frequency_collection = FREQUENCY.generated_collection(frequency_scenario)
        FREQUENCY.open_generated_evidence_segment(
            frequency_collection["epochs"], 4, "fixture-segment-b"
        )
        FREQUENCY.open_generated_evidence_segment(
            frequency_collection["epochs"], 8, "fixture-segment-c"
        )
        frequency_records: list[dict[str, Any]] = []
        fixture_frequency_root = Path(__file__).resolve().parent / "fixtures" / (
            "knife15-m2-frequency"
        )
        for index, generated_epoch in enumerate(frequency_collection["epochs"]):
            run_number = index // 4
            run_id = f"frequency-fixture-run-{run_number + 1}"
            mac_sha = hashlib.sha256(run_id.encode()).hexdigest()
            exit_sha = hashlib.sha256(f"exit-{run_id}".encode()).hexdigest()
            prefix = artifact_root / mac_sha
            epoch_results = copy.deepcopy(generated_epoch["tcp_results"])
            for result in epoch_results:
                source_result = fixture_frequency_root / result["path"]
                result["path"] = (
                    f"m2-frequency/epoch_{index % 4 + 1:03d}/{result['path']}"
                )
                destination = prefix / result["path"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                if not destination.exists():
                    destination.write_bytes(source_result.read_bytes())
            raw_epoch = {
                "schema": FREQUENCY_RAW_EPOCH_SCHEMA,
                "epoch_id": generated_epoch["epoch_id"],
                "run_id": run_id,
                "run_epoch_index": index % 4,
                "started_utc": generated_epoch["started_utc"],
                "ended_utc": generated_epoch["ended_utc"],
                "started_monotonic_ns": generated_epoch["started_monotonic_ns"],
                "ended_monotonic_ns": generated_epoch["ended_monotonic_ns"],
                "identity": generated_epoch["identity"],
                "tcp_results": epoch_results,
                "safety": {
                    "schema": FREQUENCY_SAFETY_SCHEMA,
                    "status": "PASS",
                    "tcp_results": len(generated_epoch["tcp_results"]),
                    "udp_results": 1,
                    "tcp_max_gap_bytes": 0,
                    "udp_max_loss_percent": "0.000000",
                    "invalid_result_files": 0,
                    "dns_results": 1,
                    "invalid_dns_results": 0,
                    "real_client_results": 1,
                    "invalid_real_client_results": 0,
                    "process_samples": 720,
                    "network_samples": 720,
                    "endpoint_samples": 720,
                    "endpoint_conservation_max_bytes": 61_440,
                    "endpoint_live_bytes": 0,
                    "endpoint_outstanding_bytes": 0,
                    "resource_binding_pass": True,
                    "observer_match": True,
                    "network_control_pass": True,
                    "d16_terminal_ownership_pass": True,
                    "recovery_safety_pass": True,
                    "interface_error_samples": 0,
                    "physical_interface_error_samples": 0,
                    "log_compactions": 0,
                    "internal_failure_matches": 0,
                },
            }
            frequency_records.append(
                {
                    "schema": FREQUENCY_RECORD_SCHEMA,
                    "sequence": index + 1,
                    "raw_epoch": raw_epoch,
                    "raw_epoch_sha256": canonical_sha256(raw_epoch),
                    "mac_bundle_name": (
                        f"mini_vpn_knife15_macos_frequency_{run_number + 1}.tar.gz"
                    ),
                    "mac_bundle_sha256": mac_sha,
                    "exit_bundle_name": (
                        f"mini_vpn_knife15_exit_frequency_{run_number + 1}.tar.gz"
                    ),
                    "exit_bundle_sha256": exit_sha,
                }
            )
        frequency_ledger = {
            "schema": FREQUENCY_LEDGER_SCHEMA,
            "tier_a_ledger_sha256": TIER_A_EXHAUSTED_LEDGER_SHA256,
            "tier_a_evaluation_sha256": TIER_A_EXHAUSTED_EVALUATION_SHA256,
            "epochs": frequency_records,
        }
        frequency_result = evaluate_frequency_ledger(
            frequency_ledger, artifact_root
        )
        assert frequency_result["status"] == "TIER_B_ACCEPTED"
        assert frequency_result["valid_epochs"] == 12
        assert frequency_result["max_contiguous_valid_epochs_same_lifetime"] == 4
        pending_frequency = copy.deepcopy(frequency_ledger)
        pending_frequency["epochs"] = pending_frequency["epochs"][:11]
        assert evaluate_frequency_ledger(
            pending_frequency, artifact_root
        )["status"] == "TIER_B_PENDING"
        reused_exit = copy.deepcopy(frequency_ledger)
        for record in reused_exit["epochs"][8:12]:
            record["exit_bundle_name"] = reused_exit["epochs"][0][
                "exit_bundle_name"
            ]
            record["exit_bundle_sha256"] = reused_exit["epochs"][0][
                "exit_bundle_sha256"
            ]
        try:
            evaluate_frequency_ledger(reused_exit, artifact_root)
        except LedgerError:
            pass
        else:
            raise AssertionError("Tier-B ledger reused one Exit capture across runs")
        unsafe_frequency = copy.deepcopy(frequency_ledger)
        unsafe_frequency["epochs"][0]["raw_epoch"]["safety"][
            "udp_max_loss_percent"
        ] = "3.000001"
        unsafe_frequency["epochs"][0]["raw_epoch_sha256"] = canonical_sha256(
            unsafe_frequency["epochs"][0]["raw_epoch"]
        )
        try:
            evaluate_frequency_ledger(unsafe_frequency, artifact_root)
        except LedgerError:
            pass
        else:
            raise AssertionError("Tier-B ledger accepted a non-continuity gate failure")
        builder_raw = frequency_records[0]["raw_epoch"]
        builder_identity = artifact_root / "frequency-identity.json"
        builder_safety = artifact_root / "frequency-safety.json"
        builder_index = artifact_root / "frequency-index.tsv"
        builder_identity.write_text(
            canonical_json(builder_raw["identity"]) + "\n", encoding="utf-8"
        )
        builder_safety.write_text(
            canonical_json(builder_raw["safety"]) + "\n", encoding="utf-8"
        )
        builder_index.write_text(
            "result_id\tpath\treverse\tstarted_utc\tstarted_monotonic_ns\n"
            + "\n".join(
                "\t".join(
                    (
                        result["result_id"],
                        result["path"],
                        str(result["reverse"]),
                        result["started_utc"],
                        str(result["started_monotonic_ns"]),
                    )
                )
                for result in builder_raw["tcp_results"]
            )
            + "\n",
            encoding="utf-8",
        )
        rebuilt = build_raw_frequency_epoch(
            identity_path=builder_identity,
            safety_path=builder_safety,
            index_path=builder_index,
            evidence_root=artifact_root / frequency_records[0]["mac_bundle_sha256"],
            epoch_id=builder_raw["epoch_id"],
            run_id=builder_raw["run_id"],
            run_epoch_index=builder_raw["run_epoch_index"],
            started_utc=builder_raw["started_utc"],
            ended_utc=builder_raw["ended_utc"],
            started_monotonic_ns=builder_raw["started_monotonic_ns"],
            ended_monotonic_ns=builder_raw["ended_monotonic_ns"],
        )
        assert rebuilt == builder_raw
        misplaced_result = copy.deepcopy(builder_raw)
        misplaced_result["tcp_results"][0]["path"] = (
            "m2-frequency/epoch_002/results/partial-final-zero.json"
        )
        try:
            validate_raw_frequency_epoch(misplaced_result)
        except LedgerError:
            pass
        else:
            raise AssertionError("frequency epoch accepted another epoch's TCP result")

        replayable_frequency = make_valid_frequency_record(artifact_root)
        validate_frequency_record(
            replayable_frequency, artifact_root, verify_artifacts=True
        )
        with tempfile.TemporaryDirectory(
            prefix="knife15-frequency-parser-self-test-"
        ) as materialized:
            validate_and_materialize_frequency_record(
                replayable_frequency, artifact_root, Path(materialized)
            )

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

        runner_bridge = json.loads(canonical_json(ledgers["one-formal-pass.json"]))
        runner_bridge["attempts"][0]["source_commit"] = "1" * 40
        runner_bridge["attempts"][0]["binary_sha256"] = (
            "5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032"
        )
        runner_bridge["attempts"][0]["runner_sha256"] = (
            "c55dc940d974539f98e2f449387159965972249dcda476fad451c44d3466e192"
        )
        runner_bridge["attempts"][1]["source_commit"] = "2" * 40
        runner_bridge["attempts"][1]["binary_sha256"] = (
            "5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032"
        )
        runner_bridge["attempts"][1]["runner_sha256"] = (
            "8b0d0c9ed220075481f4459dbfbde7d8ef88738d9ca6f2afc802b65d96f25a86"
        )
        evaluate(runner_bridge, artifact_root, verify_contents=False)

        wrong_binary_bridge = json.loads(canonical_json(runner_bridge))
        for attempt in wrong_binary_bridge["attempts"]:
            attempt["binary_sha256"] = "8" * 64
        try:
            evaluate(wrong_binary_bridge, artifact_root, verify_contents=False)
        except LedgerError:
            pass
        else:
            raise AssertionError(
                "checkpoint reader bridge accepted a different release binary"
            )

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
    exhaustion_parser = subparsers.add_parser("verify-exhaustion")
    exhaustion_parser.add_argument("--ledger", type=Path, required=True)
    exhaustion_parser.add_argument("--evaluation", type=Path, required=True)
    exhaustion_parser.add_argument("--output", type=Path)
    frequency_resource_parser = subparsers.add_parser(
        "frequency-resource-identity"
    )
    frequency_resource_parser.add_argument("--candidate", type=Path, required=True)
    build_frequency_parser = subparsers.add_parser("build-frequency-epoch")
    build_frequency_parser.add_argument("--identity", type=Path, required=True)
    build_frequency_parser.add_argument("--safety", type=Path, required=True)
    build_frequency_parser.add_argument("--index", type=Path, required=True)
    build_frequency_parser.add_argument("--evidence-root", type=Path, required=True)
    build_frequency_parser.add_argument("--epoch-id", required=True)
    build_frequency_parser.add_argument("--run-id", required=True)
    build_frequency_parser.add_argument("--run-epoch-index", type=int, required=True)
    build_frequency_parser.add_argument("--started-utc", required=True)
    build_frequency_parser.add_argument("--ended-utc", required=True)
    build_frequency_parser.add_argument(
        "--started-monotonic-ns", type=int, required=True
    )
    build_frequency_parser.add_argument(
        "--ended-monotonic-ns", type=int, required=True
    )
    build_frequency_parser.add_argument("--output", type=Path, required=True)
    raw_frequency_parser = subparsers.add_parser("evaluate-frequency-run")
    raw_frequency_parser.add_argument("--epochs-dir", type=Path, required=True)
    raw_frequency_parser.add_argument("--evidence-root", type=Path, required=True)
    raw_frequency_parser.add_argument("--output", type=Path)
    seal_frequency_parser = subparsers.add_parser("seal-frequency-epoch")
    seal_frequency_parser.add_argument("--artifact-root", type=Path, required=True)
    seal_frequency_parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent
    )
    seal_frequency_parser.add_argument("--sequence", type=int, required=True)
    seal_frequency_parser.add_argument(
        "--run-epoch-index", type=int, required=True
    )
    seal_frequency_parser.add_argument("--mac-bundle", required=True)
    seal_frequency_parser.add_argument("--mac-sha256", required=True)
    seal_frequency_parser.add_argument("--exit-bundle", required=True)
    seal_frequency_parser.add_argument("--exit-sha256", required=True)
    seal_frequency_parser.add_argument("--output", type=Path, required=True)
    new_frequency_parser = subparsers.add_parser("new-frequency-ledger")
    new_frequency_parser.add_argument("--output", type=Path, required=True)
    append_frequency_parser = subparsers.add_parser("append-frequency-epoch")
    append_frequency_parser.add_argument("--ledger", type=Path, required=True)
    append_frequency_parser.add_argument("--record", type=Path, required=True)
    append_frequency_parser.add_argument("--artifact-root", type=Path, required=True)
    append_frequency_parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent
    )
    append_frequency_parser.add_argument("--output", type=Path, required=True)
    evaluate_frequency_parser = subparsers.add_parser("evaluate-frequency")
    evaluate_frequency_parser.add_argument("ledger", type=Path)
    evaluate_frequency_parser.add_argument(
        "--artifact-root", type=Path, required=True
    )
    evaluate_frequency_parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent
    )
    evaluate_frequency_parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "frequency-resource-identity":
        candidate_path = args.candidate
        if (
            candidate_path.is_symlink()
            or not candidate_path.is_file()
            or candidate_path.stat().st_size > 64 * 1024
        ):
            raise LedgerError("frequency resource candidate is missing or unsafe")
        candidate = exact_object(
            read_json(candidate_path),
            RESOURCE_PROFILE_FIELDS,
            "frequency resource candidate",
        )
        if candidate["schema"] != "knife15-m2-resource-profile-v1":
            raise LedgerError("frequency resource candidate schema mismatch")
        sys.stdout.write(resource_identity(candidate) + "\n")
        return 0
    if args.command == "verify-exhaustion":
        result = verify_exhaustion_certificate(args.ledger, args.evaluation)
        encoded = canonical_json(result) + "\n"
        if args.output is None:
            sys.stdout.write(encoded)
        elif args.output.exists():
            raise LedgerError("refuse to replace an existing exhaustion admission result")
        else:
            args.output.write_text(encoded, encoding="utf-8")
        return 0
    if args.command == "build-frequency-epoch":
        evidence_root = args.evidence_root.resolve(strict=True)
        if not evidence_root.is_dir():
            raise LedgerError("frequency evidence root must be a directory")
        result = build_raw_frequency_epoch(
            identity_path=args.identity,
            safety_path=args.safety,
            index_path=args.index,
            evidence_root=evidence_root,
            epoch_id=args.epoch_id,
            run_id=args.run_id,
            run_epoch_index=args.run_epoch_index,
            started_utc=args.started_utc,
            ended_utc=args.ended_utc,
            started_monotonic_ns=args.started_monotonic_ns,
            ended_monotonic_ns=args.ended_monotonic_ns,
        )
        if args.output.exists():
            raise LedgerError("refuse to replace an existing frequency epoch")
        args.output.write_text(canonical_json(result) + "\n", encoding="utf-8")
        return 0
    if args.command == "evaluate-frequency-run":
        epochs_dir = args.epochs_dir.resolve(strict=True)
        evidence_root = args.evidence_root.resolve(strict=True)
        if not epochs_dir.is_dir() or not evidence_root.is_dir():
            raise LedgerError("raw frequency run paths must be directories")
        epoch_paths = sorted(epochs_dir.glob("epoch_*/epoch.json"))
        if not epoch_paths or len(epoch_paths) > 4 or any(
            path.is_symlink() for path in epoch_paths
        ):
            raise LedgerError("raw frequency run epoch set is invalid")
        result = summarize_raw_frequency_run(
            [read_json(path) for path in epoch_paths], evidence_root
        )
        encoded = canonical_json(result) + "\n"
        if args.output is None:
            sys.stdout.write(encoded)
        elif args.output.exists():
            raise LedgerError("refuse to replace a raw frequency run result")
        else:
            args.output.write_text(encoded, encoding="utf-8")
        return 0
    if args.command == "new-frequency-ledger":
        if args.output.exists():
            raise LedgerError("refuse to replace an existing frequency ledger")
        result = {
            "schema": FREQUENCY_LEDGER_SCHEMA,
            "tier_a_ledger_sha256": TIER_A_EXHAUSTED_LEDGER_SHA256,
            "tier_a_evaluation_sha256": TIER_A_EXHAUSTED_EVALUATION_SHA256,
            "epochs": [],
        }
        args.output.write_text(canonical_json(result) + "\n", encoding="utf-8")
        return 0
    if args.command in {
        "seal-frequency-epoch",
        "append-frequency-epoch",
        "evaluate-frequency",
    }:
        artifact_root = args.artifact_root.resolve(strict=True)
        if not artifact_root.is_dir():
            raise LedgerError("frequency artifact root must resolve to a directory")
        if args.command == "seal-frequency-epoch":
            result = derive_frequency_record(
                artifact_root=artifact_root,
                repo=args.repo,
                sequence=args.sequence,
                run_epoch_index=args.run_epoch_index,
                mac_bundle_name=args.mac_bundle,
                mac_bundle_sha256=args.mac_sha256,
                exit_bundle_name=args.exit_bundle,
                exit_bundle_sha256=args.exit_sha256,
            )
            if args.output.exists():
                raise LedgerError("refuse to replace an existing frequency record")
            args.output.write_text(canonical_json(result) + "\n", encoding="utf-8")
            return 0
        if args.command == "append-frequency-epoch":
            ledger = read_json(args.ledger)
            exact_object(ledger, FREQUENCY_LEDGER_FIELDS, "frequency ledger")
            record = read_json(args.record)
            if not isinstance(ledger.get("epochs"), list):
                raise LedgerError("frequency ledger epochs must be an array")
            candidate = copy.deepcopy(ledger)
            candidate["epochs"].append(record)
            evaluate_frequency_ledger(
                candidate, artifact_root, verify_artifacts=True
            )
            for item in candidate["epochs"]:
                validate_frequency_source_tree(item, args.repo)
            if args.output.exists():
                raise LedgerError("refuse to replace an existing frequency ledger")
            args.output.write_text(canonical_json(candidate) + "\n", encoding="utf-8")
            return 0
        ledger = read_json(args.ledger)
        result = evaluate_frequency_ledger(
            ledger, artifact_root, verify_artifacts=True
        )
        for record in ledger["epochs"]:
            validate_frequency_source_tree(record, args.repo)
        encoded = canonical_json(result) + "\n"
        if args.output is None:
            sys.stdout.write(encoded)
        elif args.output.exists():
            raise LedgerError("refuse to replace an existing frequency result")
        else:
            args.output.write_text(encoded, encoding="utf-8")
        return 0
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
