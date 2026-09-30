#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate public machine contracts and their shared conformance vectors."""
from __future__ import annotations
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


SCHEMA_CASES = (
    (
        "Tenon Document v1 Schema",
        CONTRACTS / "tenon-document" / "v1.schema.json",
        CONTRACTS / "tenon-document" / "test-fixtures" / "v1.test-vectors.json",
        "document",
    ),
    (
        "Runner configuration Schema",
        CONTRACTS / "runner" / "config.schema.json",
        CONTRACTS / "runner" / "test-fixtures" / "config.schema-test-vectors.json",
        "config",
    ),
    (
        "Plugin manifest Schema",
        CONTRACTS / "plugin" / "manifest.schema.json",
        CONTRACTS / "plugin" / "test-fixtures" / "manifest.test-vectors.json",
        "manifest",
    ),
)

class ValidationFailure(Exception):
    pass

class DuplicateKey(Exception):
    pass

def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(key)
        result[key] = value
    return result

def read_text(path: Path) -> str:
    try:
        data = path.read_bytes()
        if data.startswith(b"\xef\xbb\xbf"):
            raise ValidationFailure(
                f"{path.relative_to(ROOT)} must not contain a UTF-8 BOM"
            )
        return data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValidationFailure(
            f"{path.relative_to(ROOT)} is not valid UTF-8: {error}"
        ) from error

def load_json(path: Path) -> object:
    try:
        return json.loads(read_text(path), object_pairs_hook=reject_duplicate_keys)
    except DuplicateKey as error:
        raise ValidationFailure(
            f"{path.relative_to(ROOT)} contains a duplicate JSON key: {error}"
        ) from error
    except json.JSONDecodeError as error:
        raise ValidationFailure(
            f"{path.relative_to(ROOT)} is not valid JSON: {error}"
        ) from error

def validate_internal_schema_refs(label: str, schema: object) -> None:
    pending = [schema]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            reference = value.get("$ref")
            if reference is not None and (
                not isinstance(reference, str) or not reference.startswith("#")
            ):
                raise ValidationFailure(
                    f"{label} may use only internal $ref values: {reference!r}"
                )
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)

def validate_schema_explanatory_text(label: str, schema: object) -> None:
    pending = [("", schema)]
    while pending:
        pointer, value = pending.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                child_pointer = f"{pointer}/{key}"
                if (pointer.rsplit("/", 1)[-1] not in {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
                        and key in {"title", "description"}) and (
                    not isinstance(child, str) or not child or not child.isascii()
                ):
                    raise ValidationFailure(
                        f"{label} explanatory text must be non-empty English ASCII at "
                        f"{child_pointer}"
                    )
                pending.append((child_pointer, child))
        elif isinstance(value, list):
            pending.extend(
                (f"{pointer}/{index}", child)
                for index, child in enumerate(value)
            )

def validate_schema_vectors(
    label: str,
    schema: object,
    vectors_path: Path,
    validator_type: type,
    value_key: str,
) -> None:
    vectors = load_json(vectors_path)
    if not isinstance(vectors, dict):
        raise ValidationFailure(f"The {label} vector root must be an object")
    valid_vectors = vectors.get("valid")
    invalid_vectors = vectors.get("invalid")
    if not isinstance(valid_vectors, list) or not valid_vectors:
        raise ValidationFailure(f"{label} vectors need a non-empty valid list")
    if not isinstance(invalid_vectors, list) or not invalid_vectors:
        raise ValidationFailure(f"{label} vectors need a non-empty invalid list")

    validator = validator_type(schema)
    names: set[str] = set()
    for expected_valid, entries in ((True, valid_vectors), (False, invalid_vectors)):
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                raise ValidationFailure(f"{label} vector {index} has no name")
            expected_keys = (
                {"name", value_key}
                if expected_valid
                else {"name", value_key, "expectedInstancePointer"}
            )
            if set(entry) != expected_keys:
                raise ValidationFailure(
                    f"{label} vector {entry['name']} has unexpected fields"
                )
            if entry["name"] in names:
                raise ValidationFailure(f"{label} vector names must be unique")
            names.add(entry["name"])

            errors = list(validator.iter_errors(entry[value_key]))
            if expected_valid:
                if errors:
                    raise ValidationFailure(
                        f"Valid {label} vector {entry['name']} was rejected: "
                        f"{errors[0].message}"
                    )
                continue
            if not errors:
                raise ValidationFailure(
                    f"Invalid {label} vector {entry['name']} produced {len(errors)} errors"
                )
            expected_pointer = entry["expectedInstancePointer"]
            if not isinstance(expected_pointer, str) or (
                expected_pointer and not expected_pointer.startswith("/")
            ):
                raise ValidationFailure(
                    f"Invalid {label} vector {entry['name']} has an invalid pointer"
                )
            actual_pointers = {
                "".join(
                    "/" + str(segment).replace("~", "~0").replace("/", "~1")
                    for segment in error.absolute_path
                )
                for error in errors
            }
            if actual_pointers != {expected_pointer}:
                raise ValidationFailure(
                    f"Invalid {label} vector {entry['name']} error location changed: "
                    f"expected {expected_pointer!r}, got {sorted(actual_pointers)!r}"
                )

def validate_json_and_schemas() -> int:
    json_paths = sorted(CONTRACTS.rglob("*.json"))
    for path in json_paths:
        load_json(path)

    try:
        from jsonschema import Draft202012Validator
    except ImportError as error:
        raise ValidationFailure(
            "jsonschema is required to validate Draft 2020-12 schemas"
        ) from error

    schema_paths = {path for path in CONTRACTS.rglob("*.schema.json")}
    registered_schema_paths = {case[1] for case in SCHEMA_CASES}
    if schema_paths != registered_schema_paths:
        raise ValidationFailure(
            "Schema validation cases are incomplete: "
            f"missing={sorted(str(path.relative_to(ROOT)) for path in schema_paths - registered_schema_paths)}, "
            f"extra={sorted(str(path.relative_to(ROOT)) for path in registered_schema_paths - schema_paths)}"
        )

    for label, schema_path, vectors_path, value_key in SCHEMA_CASES:
        schema = load_json(schema_path)
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as error:
            raise ValidationFailure(f"{label} is invalid: {error}") from error
        validate_internal_schema_refs(label, schema)
        validate_schema_explanatory_text(label, schema)
        validate_schema_vectors(
            label, schema, vectors_path, Draft202012Validator, value_key
        )
    return len(json_paths)

def main() -> int:
    try:
        count = validate_json_and_schemas()
    except ValidationFailure as error:
        print(f"Contract validation failed: {error}", file=sys.stderr)
        return 1
    print(f"Contract validation passed: {count} JSON files and {len(SCHEMA_CASES)} schemas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
