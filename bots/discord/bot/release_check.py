from __future__ import annotations

import ast
import json
import stat
from pathlib import Path
from typing import Any

from .buildmeta import build_runtime_drift_report, load_build_info, load_change_journal, read_pyproject_version
from .config_schema import DEFAULTS
from .event_contracts import EVENT_CONTRACT_VERSION

ROOT_DIR = Path(__file__).resolve().parent.parent


def _load_required_manifest(errors: list[str]) -> dict[str, Any]:
    manifest_path = ROOT_DIR / "release_required_files.json"
    if not manifest_path.exists():
        errors.append("release_required_files.json is missing")
        return {"required_files": [], "executable_files": []}
    manifest = _read_json(manifest_path, errors)
    required_files = manifest.get("required_files")
    executable_files = manifest.get("executable_files")
    if not isinstance(required_files, list) or not all(isinstance(item, str) for item in required_files):
        errors.append("release_required_files.json: required_files must be a list of strings")
        required_files = []
    if not isinstance(executable_files, list) or not all(isinstance(item, str) for item in executable_files):
        errors.append("release_required_files.json: executable_files must be a list of strings")
        executable_files = []
    return {**manifest, "required_files": required_files, "executable_files": executable_files}


def _read_json(path: Path, errors: list[str]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{path}: invalid JSON: {exc}")
        return {}
    if not isinstance(payload, dict):
        errors.append(f"{path}: top-level JSON must be an object")
        return {}
    return payload



def _read_schema_metadata(errors: list[str]) -> tuple[int, list[tuple[int, str]]]:
    source_path = ROOT_DIR / "nmbot" / "community_store.py"
    try:
        source = source_path.read_text(encoding="utf-8")
        module = ast.parse(source)
    except Exception as exc:
        errors.append(f"{source_path}: failed to parse schema metadata: {exc}")
        return 0, []
    schema_version = 0
    migrations: list[tuple[int, str]] = []
    for node in module.body:
        targets: list[str] = []
        value_node: ast.AST | None = None
        if isinstance(node, ast.Assign):
            targets = [target.id for target in node.targets if isinstance(target, ast.Name)]
            value_node = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target.id]
            value_node = node.value
        if value_node is None:
            continue
        if "SCHEMA_VERSION" in targets:
            try:
                schema_version = int(ast.literal_eval(value_node))
            except Exception as exc:
                errors.append(f"SCHEMA_VERSION is not a literal integer: {exc}")
        if "COMMUNITY_SCHEMA_MIGRATIONS" in targets:
            try:
                raw = ast.literal_eval(value_node)
                migrations = [(int(version), str(name)) for version, name in raw]
            except Exception as exc:
                errors.append(f"COMMUNITY_SCHEMA_MIGRATIONS is not literal: {exc}")
    if not schema_version:
        errors.append("SCHEMA_VERSION not found in community_store.py")
    if not migrations:
        errors.append("COMMUNITY_SCHEMA_MIGRATIONS not found in community_store.py")
    return schema_version, migrations

def _extract_config_env_refs(path: Path) -> set[str]:
    source = path.read_text(encoding="utf-8")
    module = ast.parse(source)
    refs: set[str] = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id.startswith("_get_"):
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                refs.add(node.args[0].value)
        elif isinstance(node.func, ast.Attribute) and node.func.attr == "getenv":
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                refs.add(node.args[0].value)
    return refs


def _is_executable(path: Path) -> bool:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return False
    return bool(mode & stat.S_IXUSR)


def run_release_check(settings: Any, runtime_version: str) -> int:
    errors: list[str] = []
    warnings: list[str] = []

    content_json = _read_json(ROOT_DIR / "templates" / "content.json", errors)
    layout_json = _read_json(ROOT_DIR / "templates" / "server_layout.json", errors)
    build_info = load_build_info()
    change_journal = load_change_journal()

    drift = build_runtime_drift_report(settings, runtime_version)
    errors.extend(str(item) for item in drift.get("errors") or [])
    warnings.extend(str(item) for item in drift.get("warnings") or [])

    pyproject_version = read_pyproject_version()
    if pyproject_version != runtime_version:
        errors.append(f"version mismatch: pyproject.toml={pyproject_version}, runtime={runtime_version}")
    if str(build_info.get("version") or "") != runtime_version:
        errors.append(f"version mismatch: build_info.json={build_info.get('version')}, runtime={runtime_version}")
    if str(change_journal.get("version") or "") != runtime_version:
        warnings.append(f"change_journal.json version is {change_journal.get('version')}, runtime is {runtime_version}")

    schema_version, schema_migrations = _read_schema_metadata(errors)
    highest_migration = max((version for version, _name in schema_migrations), default=0)
    if schema_version != highest_migration:
        errors.append(
            "SCHEMA_VERSION must match the highest COMMUNITY_SCHEMA_MIGRATIONS version "
            f"({schema_version} != {highest_migration})"
        )
    try:
        build_schema_version = int(build_info.get("schema_version") or 0)
    except Exception:
        build_schema_version = 0
    if build_schema_version and build_schema_version != schema_version:
        errors.append(f"build_info.json schema_version={build_schema_version}, runtime schema={schema_version}")

    if str(build_info.get("event_contract_version") or "") != str(EVENT_CONTRACT_VERSION):
        errors.append(
            "build_info.json event_contract_version="
            f"{build_info.get('event_contract_version')}, runtime event contract={EVENT_CONTRACT_VERSION}"
        )

    config_refs = _extract_config_env_refs(ROOT_DIR / "nmbot" / "config.py")
    missing_from_schema = sorted(config_refs - set(DEFAULTS))
    if missing_from_schema:
        errors.append("config_schema.py is missing env keys used by config.py: " + ", ".join(missing_from_schema))
    if "COMMAND_SURFACE_MODE" not in set(DEFAULTS):
        errors.append("config_schema.py must define COMMAND_SURFACE_MODE")

    required_manifest = _load_required_manifest(errors)
    required_files = tuple(required_manifest.get("required_files") or ())
    for required_file in required_files:
        if not (ROOT_DIR / required_file).exists():
            errors.append(f"required production-safety file is missing: {required_file}")

    for baseline_required in (
        "nmbot/config_schema.py", "nmbot/release_check.py", "nmbot/release_pack.py",
        "nmbot/schema_doctor.py", "nmbot/env_doctor.py", "nmbot/discord_bindings.py",
        "nmbot/bridge_doctor.py", "nmbot/bot_bridge_runtime.py", "nmbot/bot_extensions.py",
    ):
        if baseline_required not in required_files:
            errors.append(f"release_required_files.json must include {baseline_required}")

    setup_source = (ROOT_DIR / "setup_wizard.py").read_text(encoding="utf-8")
    if "from nmbot.config_schema import DEFAULTS" not in setup_source:
        errors.append("setup_wizard.py must use nmbot.config_schema as the env source of truth")
    if "PROFILE_KEYS" not in setup_source or "--validate-only" not in setup_source or "--non-interactive" not in setup_source:
        errors.append("setup_wizard.py must expose PROFILE_KEYS, --validate-only and --non-interactive")

    for script in ("install.sh", "run.sh", "upgrade.sh", "setup_wizard.py"):
        if not _is_executable(ROOT_DIR / script):
            errors.append(f"{script} is not executable")

    for forbidden in (".github", "tests"):
        if (ROOT_DIR / forbidden).exists():
            errors.append(f"forbidden release path exists: {forbidden}")
    if (ROOT_DIR / ".env.example").exists():
        errors.append(".env.example must not be present in this production archive")

    readme = (ROOT_DIR / "README.md").read_text(encoding="utf-8")
    if f"Runtime version: {runtime_version}" not in readme:
        errors.append(f"README.md must declare Runtime version: {runtime_version}")
    if "BRIDGE_DESTINATION_RULES_JSON" in readme:
        errors.append("README.md references deprecated BRIDGE_DESTINATION_RULES_JSON; use BRIDGE_EVENT_RULES_JSON")
    if "--release-check" not in readme:
        warnings.append("README.md should document python -m nmbot.main --release-check")
    if "--schema-doctor" not in readme:
        warnings.append("README.md should document python -m nmbot.main --schema-doctor")
    if "nmbot.release_pack" not in readme:
        warnings.append("README.md should document python -m nmbot.release_pack")
    if "--bridge-doctor" not in readme:
        warnings.append("README.md should document python -m nmbot.main --bridge-doctor")
    if "--profile integrations" not in readme:
        warnings.append("README.md should document setup_wizard.py --profile integrations")

    for marker in ("build_profile", "patch_level", "source_tree_state"):
        if not build_info.get(marker):
            errors.append(f"build_info.json must include non-empty {marker}")

    payload = {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "runtime_version": runtime_version,
        "schema_version": schema_version,
        "event_contract_version": EVENT_CONTRACT_VERSION,
        "content_meta": content_json.get("meta") if isinstance(content_json, dict) else {},
        "layout_meta": layout_json.get("meta") if isinstance(layout_json, dict) else {},
        "env_schema_keys": len(DEFAULTS),
        "checked_scripts": ["install.sh", "run.sh", "upgrade.sh", "setup_wizard.py"],
        "required_files_checked": len(required_files),
        "required_manifest": required_manifest.get("schema_version"),
        "build_profile": build_info.get("build_profile"),
        "patch_level": build_info.get("patch_level"),
        "source_tree_state": build_info.get("source_tree_state"),
        "release_check_dependency_light": True,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not errors else 4
