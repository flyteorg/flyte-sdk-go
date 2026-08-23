"""hello: a Go task, run as a Flyte task.

    flyte run task.py my_task --x 21 --label demo

Nothing here declares the task's inputs or outputs — they come from the worker
binary itself (`go build -o bin/hello . && ./bin/hello describe-interface`).
"""

from pathlib import Path

import flyteplugins_go as fgo

_MODULE = Path(__file__).resolve().parent

my_task, go_env = fgo.go_task(
    module_dir=_MODULE,
    binary="hello",
)
