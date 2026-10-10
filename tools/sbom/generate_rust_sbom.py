#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 SBOM for the locked Rust driver dependency graph.

Runs `cargo metadata --locked --offline` so the SBOM reflects exactly the
committed Cargo.lock. No network access and no package publication.

Usage: generate_rust_sbom.py [output-directory]   (default: <repo>/sbom-out)
"""
import json
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    out_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "sbom-out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "justybase-db2.cdx.json"

    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
            str(ROOT / "Cargo.toml"),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(result.stderr.strip(), file=sys.stderr)
        print("cargo metadata failed; prepare the locked dependency cache with cargo fetch --locked", file=sys.stderr)
        return result.returncode
    metadata = json.loads(result.stdout)

    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    workspace_ids = set(metadata["workspace_members"])

    components = []
    for package_id in sorted(packages):
        package = packages[package_id]
        if package_id in workspace_ids:
            continue
        component = {
            "type": "library",
            "bom-ref": f"pkg:cargo/{package['name']}@{package['version']}",
            "name": package["name"],
            "version": package["version"],
            "purl": f"pkg:cargo/{package['name']}@{package['version']}",
            "scope": "required",
        }
        if package.get("license"):
            component["licenses"] = [{"license": {"name": package["license"]}}]
        elif package.get("license_file"):
            component["licenses"] = [{"license": {"name": package["license_file"]}}]
        components.append(component)

    root_ref = None
    for package_id in workspace_ids:
        package = packages[package_id]
        if package["name"] == "justybase-db2":
            root_ref = f"pkg:cargo/{package['name']}@{package['version']}"
            break
    if root_ref is None:
        print("justybase-db2 root package not found in workspace", file=sys.stderr)
        return 1

    dependencies = []
    for node in nodes.values():
        ref = f"pkg:cargo/{packages[node['id']]['name']}@{packages[node['id']]['version']}"
        depends_on = sorted(
            f"pkg:cargo/{packages[dep['pkg']]['name']}@{packages[dep['pkg']]['version']}"
            for dep in node.get("deps", [])
        )
        dependencies.append({"ref": ref, "dependsOn": depends_on})

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "tools": [{"vendor": "db2-drivers", "name": "generate_rust_sbom.py"}],
            "component": {
                "type": "library",
                "bom-ref": root_ref,
                "name": "justybase-db2",
                "version": packages[
                    next(
                        pid
                        for pid in workspace_ids
                        if packages[pid]["name"] == "justybase-db2"
                    )
                ]["version"],
                "purl": root_ref,
            },
        },
        "components": components,
        "dependencies": dependencies,
    }
    out.write_text(json.dumps(sbom, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {out} with {len(components)} components")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
