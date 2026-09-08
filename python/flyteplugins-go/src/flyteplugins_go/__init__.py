"""Run Go Flyte tasks from Python.

A Go task container runs a compiled binary and no Python. This package is the
launch-time half: it reads a worker binary's self-described interface, declares
the matching Flyte task, and builds the worker image from Go sources — so an
example (or a user's project) needs only a few lines:

    from pathlib import Path
    import flyteplugins_go as fgo

    my_task, go_env = fgo.go_task(
        module_dir=Path(__file__).parent,
        binary="hello",
    )

The interface is never hand-written: it comes from `<binary> describe-interface`
(one JSON line per registered task), so renaming a Go input cannot silently
diverge from what gets launched. The Go counterpart of flyteplugins-rs.
"""

from ._descriptor import (
    SUPPORTED_DESCRIPTOR_VERSION,
    load_descriptors,
    native_interface,
)
from ._image import go_worker_image
from ._task import GoWorkerTask, go_task

__all__ = [
    "SUPPORTED_DESCRIPTOR_VERSION",
    "GoWorkerTask",
    "load_descriptors",
    "native_interface",
    "go_task",
    "go_worker_image",
]
