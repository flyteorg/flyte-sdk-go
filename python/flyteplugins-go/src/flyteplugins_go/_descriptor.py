"""The interface descriptors: read them from the binary, turn one into a Flyte interface.

`<binary> describe-interface` prints one JSON line per registered task, each
derived by flyteruntime.RegisterTask from the Go function signature. That output
is the single source of truth for every task's inputs and outputs.

Two sources, because module-level task declarations are also imported *inside* a
container (the Python-workflow path), where no go toolchain exists:

- a local `go build` artifact, when present — authoritative, and the bundled
  copy is refreshed from it;
- otherwise the generated `_generated_interface.py` beside the module, which the
  code bundle carries.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import subprocess
import sys
from functools import cache
from pathlib import Path
from typing import Any

from flyte.models import NativeInterface

SUPPORTED_DESCRIPTOR_VERSION = 1

_GENERATED_MODULE = "_generated_interface.py"

# The descriptor's closed set of type tags -> Python types. `struct` is msgpack
# on the wire (mashumaro-compatible), which round-trips as an untyped dict.
_PY_TYPES: dict[str, type] = {
    "integer": int,
    "float": float,
    "string": str,
    "boolean": bool,
    "struct": dict,
}


def find_binary(start: Path, binary: str) -> Path | None:
    """The locally built worker binary, or None.

    The dev convention is `go build -o bin/<binary> .` in the module directory;
    a bare `go build` dropping the binary beside the sources also counts.
    Searches `start` and its ancestors (a multi-module repo may build into the
    repo root's bin/), bounded at the enclosing repository so a miss cannot
    wander up the filesystem and match some unrelated binary of the same name.
    """
    for directory in (start, *start.parents):
        for candidate in (directory / "bin" / binary, directory / binary):
            if candidate.is_file():
                return candidate
        if (directory / ".git").exists():
            break
    return None


@cache
def _describe(binary_path: str) -> str:
    return subprocess.run(
        [binary_path, "describe-interface"], check=True, capture_output=True, text=True
    ).stdout


def load_generated_descriptors(module_dir: Path) -> list[dict[str, Any]] | None:
    """The descriptors bundled beside the task, or None if there are none yet.

    Imported rather than read as text, and registered in ``sys.modules``, because
    the default ``--copy-style loaded_modules`` bundles the modules a launch
    *loaded*. A file merely opened and parsed would not travel into the
    container, and the task would then have no interface there.
    """
    path = module_dir / _GENERATED_MODULE
    if not path.is_file():
        return None

    name = f"{__package__}._generated.{module_dir.name.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return getattr(module, "DESCRIPTORS", None)


def load_descriptors(
    *,
    module_dir: Path,
    search_from: Path,
    binary: str,
    fallback: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Every task's interface: from the binary when one is built, else bundled.

    `fallback` overrides the bundled `_generated_interface.py`, which is
    otherwise found automatically.
    """
    bundled = fallback if fallback is not None else load_generated_descriptors(module_dir)

    binary_path = find_binary(search_from, binary)
    if binary_path is None:
        if bundled is None:
            raise RuntimeError(
                f"no built {binary!r} binary and no {_GENERATED_MODULE} beside "
                f"{module_dir} — run `go build -o bin/{binary} .` once to generate it, "
                f"and commit the result so the task can also be declared where no "
                f"go toolchain exists (inside a container)"
            )
        return bundled

    descriptors = [json.loads(line) for line in _describe(str(binary_path)).splitlines() if line]
    if not descriptors:
        raise RuntimeError(f"{binary} describe-interface printed no descriptors")
    for descriptor in descriptors:
        version = descriptor.get("flyte_interface_version")
        if version != SUPPORTED_DESCRIPTOR_VERSION:
            raise RuntimeError(
                f"{binary} speaks interface version {version}, this launcher supports "
                f"{SUPPORTED_DESCRIPTOR_VERSION} — update flyteplugins-go or rebuild the worker"
            )

    if descriptors != bundled:
        _write_generated_module(module_dir, descriptors)
    return descriptors


def select_descriptor(descriptors: list[dict[str, Any]], task: str | None) -> dict[str, Any]:
    """Pick one task's descriptor: the named one, or the sole one."""
    if task is not None:
        for descriptor in descriptors:
            if descriptor["task"] == task:
                return descriptor
        names = [d["task"] for d in descriptors]
        raise RuntimeError(f"binary registers no task named {task!r} (registered: {names})")
    if len(descriptors) == 1:
        return descriptors[0]
    names = [d["task"] for d in descriptors]
    raise RuntimeError(f"binary registers {len(descriptors)} tasks; pass task= (registered: {names})")


def _write_generated_module(module_dir: Path, descriptors: list[dict[str, Any]]) -> None:
    """Refresh the bundled copy so it tracks the Go source.

    Kept as a Python module rather than a JSON file on purpose: the default
    `--copy-style loaded_modules` bundles imported *modules*, so a .json sitting
    beside the task would not travel into the container.
    """
    text = (
        "# Autogenerated from `<binary> describe-interface` — safe to delete, any\n"
        "# launch with a built binary writes it back. Committed so a container, which\n"
        "# has no go toolchain, still has an interface to declare the task from.\n"
        "DESCRIPTORS = " + repr(descriptors) + "\n"
    )
    try:
        (module_dir / _GENERATED_MODULE).write_text(text)
        print(f"refreshed {_GENERATED_MODULE} from the worker binary")
    except OSError:
        pass  # read-only checkout: the live descriptors are still what gets used


def _py_type(var: dict[str, Any]) -> type:
    try:
        return _PY_TYPES[var["type"]]
    except KeyError:
        detail = f" ({var['detail']})" if "detail" in var else ""
        raise RuntimeError(
            f"{var['name']!r} has type {var['type']!r}, which cannot be launched "
            f"from Python yet{detail}"
        ) from None


def native_interface(descriptor: dict[str, Any]) -> NativeInterface:
    """Build the Flyte interface from one task's descriptor.

    Required-ness is keyed off `inspect.Parameter.empty` — the sentinel
    `NativeInterface.required_inputs()` checks.
    """
    for var in descriptor["inputs"]:
        if not var.get("required", True):
            raise RuntimeError(
                f"input {var['name']!r} declares a default, which needs a matching "
                f"literal the descriptor does not carry yet"
            )
    return NativeInterface.from_types(
        {v["name"]: (_py_type(v), inspect.Parameter.empty) for v in descriptor["inputs"]},
        {v["name"]: _py_type(v) for v in descriptor["outputs"]},
    )
