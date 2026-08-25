"""traced: record on attempt 1, replay on the retry.

    flyte run task.py flaky --seed 7

The task fails on purpose on its first attempt after recording a slow traced
step; the retry replays the recording instantly (watch the logs for
"replaying recorded trace") and succeeds. `retries=2` is what gives the replay
a second attempt to happen in.
"""

from pathlib import Path

import flyteplugins_go as fgo

_MODULE = Path(__file__).resolve().parent

flaky, go_env = fgo.go_task(
    module_dir=_MODULE,
    binary="traced",
    # This example is a package of the SDK's root module (no go.mod of its
    # own), so the image build needs the whole module as context.
    workspace=_MODULE.parent.parent.parent,
    retries=2,
)
