#!/usr/bin/env python3
"""Validate immutable Knife15 M2 resource profiles and compare path identity."""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
import sys
from pathlib import Path
from typing import Any


PROFILE_FIELDS = {
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
    "mac_target_route_sha256",
    "mac_exit_route_sha256",
    "traceroute_sha256",
    "prior_saturation_proven",
    "prior_saturation_evidence_sha256",
    "replacement_capacity_proven",
    "replacement_capacity_evidence_sha256",
}

REQUIRED_SHA256_FIELDS = (
    "server_binary_sha256",
    "server_config_sha256",
    "observer_sha256",
    "client_binary_sha256",
    "workload_profile_sha256",
    "mac_target_route_sha256",
    "mac_exit_route_sha256",
    "traceroute_sha256",
    "provider_identity_evidence_sha256",
    "route_identity_evidence_sha256",
)


def fixture_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "knife15-m2-resource" / name


def object_value(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def string_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def boolean_value(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value


def optional_string(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    return value


def hexadecimal_value(value: Any, length: int, label: str) -> str:
    result = string_value(value, label)
    if re.fullmatch(rf"[0-9a-f]{{{length}}}", result) is None:
        raise ValueError(f"{label} must be {length} lowercase hexadecimal characters")
    return result


def token_value(value: Any, label: str) -> str:
    result = string_value(value, label)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", result) is None:
        raise ValueError(f"{label} contains unsafe or unsupported characters")
    return result


def public_ipv4_value(value: Any, label: str) -> str:
    result = string_value(value, label)
    try:
        address = ipaddress.IPv4Address(result)
    except ipaddress.AddressValueError as error:
        raise ValueError(f"{label} must be an IPv4 address") from error
    if not address.is_global:
        raise ValueError(f"{label} must be globally routable")
    return str(address)


def port_value(value: Any, label: str) -> int:
    result = positive_integer(value, label)
    if result > 65535:
        raise ValueError(f"{label} must not exceed 65535")
    return result


def profile_identity(value: Any, label: str) -> dict[str, Any]:
    profile = object_value(value, label)
    missing = sorted(PROFILE_FIELDS - profile.keys())
    unknown = sorted(profile.keys() - PROFILE_FIELDS)
    if missing:
        raise ValueError(f"{label} is missing fields: {','.join(missing)}")
    if unknown:
        raise ValueError(f"{label} has unknown fields: {','.join(unknown)}")
    if profile.get("schema") != "knife15-m2-resource-profile-v1":
        raise ValueError(f"{label}.schema is not knife15-m2-resource-profile-v1")
    for field in REQUIRED_SHA256_FIELDS:
        hexadecimal_value(profile.get(field), 64, f"{label}.{field}")
    hexadecimal_value(profile.get("source_commit"), 40, f"{label}.source_commit")
    for field in (
        "candidate_id",
        "provider",
        "resource_id",
        "region",
        "route_class",
        "route_contract_id",
        "mac_interface",
    ):
        token_value(profile.get(field), f"{label}.{field}")
    asn = positive_integer(profile.get("asn"), f"{label}.asn")
    if asn > 4_294_967_295:
        raise ValueError(f"{label}.asn exceeds the 32-bit ASN range")
    tuic_port = port_value(profile.get("tuic_port"), f"{label}.tuic_port")
    target_iperf_port = port_value(
        profile.get("target_iperf_port"), f"{label}.target_iperf_port"
    )
    prior_saturation_proven = boolean_value(
        profile.get("prior_saturation_proven"), f"{label}.prior_saturation_proven"
    )
    replacement_capacity_proven = boolean_value(
        profile.get("replacement_capacity_proven"),
        f"{label}.replacement_capacity_proven",
    )
    if prior_saturation_proven != replacement_capacity_proven:
        raise ValueError(f"{label} saturation and replacement proofs must agree")
    prior_saturation_evidence = optional_string(
        profile.get("prior_saturation_evidence_sha256"),
        f"{label}.prior_saturation_evidence_sha256",
    )
    replacement_capacity_evidence = optional_string(
        profile.get("replacement_capacity_evidence_sha256"),
        f"{label}.replacement_capacity_evidence_sha256",
    )
    for proven, evidence, field in (
        (
            prior_saturation_proven,
            prior_saturation_evidence,
            "prior_saturation_evidence_sha256",
        ),
        (
            replacement_capacity_proven,
            replacement_capacity_evidence,
            "replacement_capacity_evidence_sha256",
        ),
    ):
        if proven:
            hexadecimal_value(evidence, 64, f"{label}.{field}")
        elif evidence != "":
            raise ValueError(f"{label}.{field} must be empty when proof is false")
    return {
        "candidate_id": token_value(
            profile.get("candidate_id"), f"{label}.candidate_id"
        ),
        "provider": token_value(profile.get("provider"), f"{label}.provider"),
        "public_ipv4": public_ipv4_value(
            profile.get("public_ipv4"), f"{label}.public_ipv4"
        ),
        "asn": asn,
        "target_ipv4": public_ipv4_value(
            profile.get("target_ipv4"), f"{label}.target_ipv4"
        ),
        "target_iperf_port": target_iperf_port,
        "tuic_port": tuic_port,
        "route_class": token_value(profile.get("route_class"), f"{label}.route_class"),
        "route_contract_id": token_value(
            profile.get("route_contract_id"), f"{label}.route_contract_id"
        ),
        "prior_saturation_proven": prior_saturation_proven,
        "prior_saturation_evidence_sha256": prior_saturation_evidence,
        "replacement_capacity_proven": replacement_capacity_proven,
        "replacement_capacity_evidence_sha256": replacement_capacity_evidence,
    }


def canonical_profile_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def classify_profiles(reference: Any, candidate: Any) -> dict[str, Any]:
    reference_identity = profile_identity(reference, "reference")
    candidate_identity = profile_identity(candidate, "candidate")
    reference_profile = object_value(reference, "reference")
    candidate_profile = object_value(candidate, "candidate")

    def result(eligible: bool, reason: str) -> dict[str, Any]:
        return {
            "schema": "knife15-m2-resource-eligibility-v1",
            "eligible": eligible,
            "reason": reason,
            "reference_id": reference_identity["candidate_id"],
            "candidate_id": candidate_identity["candidate_id"],
            "reference_profile_sha256": canonical_profile_sha256(reference_profile),
            "candidate_profile_sha256": canonical_profile_sha256(candidate_profile),
        }

    if candidate_identity["target_ipv4"] != reference_identity["target_ipv4"]:
        raise ValueError("candidate.target_ipv4 does not match the reference target")
    if (
        candidate_identity["target_iperf_port"]
        != reference_identity["target_iperf_port"]
    ):
        raise ValueError(
            "candidate.target_iperf_port does not match the reference target"
        )
    if candidate_identity["candidate_id"] == reference_identity["candidate_id"]:
        return result(False, "same_candidate_id")
    if candidate_identity["public_ipv4"] == reference_identity["public_ipv4"]:
        return result(False, "same_exit_ipv4")
    provider_differs = (
        candidate_identity["provider"].casefold()
        != reference_identity["provider"].casefold()
    )
    asn_differs = candidate_identity["asn"] != reference_identity["asn"]
    if provider_differs and asn_differs:
        return result(True, "distinct_provider_and_asn")
    independent_route = (
        candidate_identity["route_class"].casefold()
        != reference_identity["route_class"].casefold()
        and candidate_identity["route_contract_id"]
        != reference_identity["route_contract_id"]
    )
    if independent_route:
        return result(True, "independent_route_contract")
    saturation_replacement = (
        candidate_identity["prior_saturation_proven"]
        and candidate_identity["prior_saturation_evidence_sha256"] != ""
        and candidate_identity["replacement_capacity_proven"]
        and candidate_identity["replacement_capacity_evidence_sha256"] != ""
    )
    if saturation_replacement:
        return result(True, "proved_saturation_replacement")
    return result(False, "equivalent_unsaturated_route")


def read_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin, object_pairs_hook=unique_object)
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=unique_object)


def validate_profile(value: Any) -> dict[str, Any]:
    identity = profile_identity(value, "profile")
    profile = object_value(value, "profile")
    return {
        "schema": "knife15-m2-resource-validation-v1",
        "candidate_id": identity["candidate_id"],
        "profile_sha256": canonical_profile_sha256(profile),
        "valid": True,
    }


def self_test() -> None:
    reference: Any = read_json(str(fixture_path("reference-33.json")))
    candidate: Any = read_json(str(fixture_path("distinct-provider.json")))
    result = classify_profiles(reference, candidate)
    assert result["eligible"] is True
    assert result["reason"] == "distinct_provider_and_asn"
    assert result["candidate_id"] == "candidate-distinct-provider"
    assert len(result["reference_profile_sha256"]) == 64
    assert len(result["candidate_profile_sha256"]) == 64
    equivalent: Any = read_json(str(fixture_path("equivalent-resize.json")))
    equivalent_result = classify_profiles(reference, equivalent)
    assert equivalent_result["eligible"] is False
    assert equivalent_result["reason"] == "equivalent_unsaturated_route"
    malformed = copy.deepcopy(candidate)
    del malformed["server_config_sha256"]
    try:
        classify_profiles(reference, malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("candidate missing server configuration hash was accepted")
    malformed = copy.deepcopy(candidate)
    malformed["server_config_sha256"] = "not-a-sha256"
    try:
        classify_profiles(reference, malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("candidate with malformed SHA-256 was accepted")
    malformed = copy.deepcopy(candidate)
    malformed["public_ipv4"] = "999.1.1.1"
    try:
        classify_profiles(reference, malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("candidate with malformed public IPv4 was accepted")
    malformed = copy.deepcopy(equivalent)
    malformed["prior_saturation_proven"] = True
    malformed["prior_saturation_evidence_sha256"] = "not-a-sha256"
    malformed["replacement_capacity_proven"] = True
    malformed["replacement_capacity_evidence_sha256"] = "also-not-a-sha256"
    try:
        classify_profiles(reference, malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("candidate with malformed saturation proof was accepted")
    saturated = copy.deepcopy(equivalent)
    saturated["prior_saturation_proven"] = True
    saturated["prior_saturation_evidence_sha256"] = "a" * 64
    saturated["replacement_capacity_proven"] = True
    saturated["replacement_capacity_evidence_sha256"] = "b" * 64
    saturated_result = classify_profiles(reference, saturated)
    assert saturated_result["eligible"] is True
    assert saturated_result["reason"] == "proved_saturation_replacement"
    independent_route = copy.deepcopy(equivalent)
    independent_route["route_class"] = "premium-route"
    independent_route["route_contract_id"] = "premium-contract-a"
    independent_result = classify_profiles(reference, independent_route)
    assert independent_result["eligible"] is True
    assert independent_result["reason"] == "independent_route_contract"
    try:
        json.loads(
            '{"schema":"first","schema":"second"}',
            object_pairs_hook=unique_object,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("resource profile with a duplicate JSON key was accepted")
    malformed = copy.deepcopy(candidate)
    malformed["candidate_id"] = "candidate\ninjected=value"
    try:
        classify_profiles(reference, malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("candidate with unsafe identifier was accepted")
    print("knife15 M2 resource profile self-test passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(
        description="Validate and compare Knife15 M2 resource profiles"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("profile", help="resource profile JSON path, or -")
    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("--reference", required=True)
    compare_parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    if args.command == "validate":
        result = validate_profile(read_json(args.profile))
    else:
        if args.reference == "-" or args.candidate == "-":
            raise ValueError("compare requires two file paths")
        result = classify_profiles(
            read_json(args.reference), read_json(args.candidate)
        )
    json.dump(result, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        AssertionError,
        json.JSONDecodeError,
        KeyError,
        NotImplementedError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
