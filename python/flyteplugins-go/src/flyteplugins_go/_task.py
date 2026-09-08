"""The Flyte task that launches a Go worker binary."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import flyte
from flyte.extend import TaskTemplate
from flyte.models import SerializationContext

from ._descriptor import load_descriptors, native_interface, select_descriptor
from ._image import go_worker_image
from ._reuse import ACTOR_TASK_TYPE, actor_custom_config
from ._reuse import validate as validate_reuse

# Not "python" (nothing python runs here) and never "raw-container" (that one
# injects the copilot data sidecar). The leaseworker resolves an unregistered
# type to the default pod plugin, which is the plain container handling we want.
GO_TASK_TYPE = "go-task"


@dataclass(kw_only=True)
class GoWorkerTask(TaskTemplate):
    """A container task whose image entrypoint is a compiled Go worker."""

    binary: str = "worker"

    async def execute(self, *args, **kwargs):
        # Only reached under mode="local" / `flyte run --local`, which would mean
        # running the container ourselves (see flyte.extras.ContainerTask).
        raise NotImplementedError("remote-only task; --local is not supported")

    def custom_config(self, sctx: SerializationContext) -> dict[str, Any]:
        # Only reusable tasks carry a `custom`, and for them it is load-bearing:
        # the fasttask plugin reads the environment's shape (replicas, TTLs,
        # parallelism, identity) out of exactly this blob. See `_reuse` for why
        # we build it here rather than letting the SDK do it.
        if self.reusable is None:
            return {}
        return actor_custom_config(self, sctx, self.reusable)

    def container_args(self, sctx: SerializationContext) -> list[str]:
        # The Go worker's arg contract, identical to the Rust worker's
        # (flyteplugins-rs container_args) plus `--task`, because one Go binary
        # can register several tasks and the worker needs to know which to run.
        #
        # - args[0] is the binary: images built from image layers have no
        #   ENTRYPOINT and the container command is empty, so k8s execs args
        #   directly; under an image with an ENTRYPOINT this token is skipped.
        # - "a0" is the leading token the Python runtime also emits; skipped.
        # - input/output paths keep their {{.input}} / {{.outputPrefix}}
        #   defaults, which the backend substitutes per action.
        # - {{.runName}} / {{.actionName}} are NOT in the backend's substitution
        #   set: the worker discards "{{...}}" values and reads RUN_NAME /
        #   ACTION_NAME from env; they stay as forward-compatible placeholders.
        # - --run-base-dir is never an arg; it arrives only as _U_RUN_BASE.
        return [
            f"/usr/local/bin/{self.binary}",
            "a0",
            "--task",
            self.name,
            "--inputs",
            sctx.input_path,
            "--outputs-path",
            sctx.output_path,
            "--run-name",
            "{{.runName}}",
            "--name",
            "{{.actionName}}",
        ]


def go_task(
    *,
    module_dir: Path,
    binary: str,
    task: str | None = None,
    fallback_descriptors: list[dict[str, Any]] | None = None,
    workspace: Path | None = None,
    dockerfile: Path | None = None,
    image_name: str | None = None,
    env_name: str | None = None,
    image: flyte.Image | None = None,
    reuse: flyte.ReusePolicy | None = None,
    platform: str | tuple[str, ...] | None = None,
    **task_kwargs: Any,
) -> tuple[GoWorkerTask, flyte.TaskEnvironment]:
    """Declare a Go worker as a Flyte task, plus the environment holding it.

    The task's name and interface come from the binary's own descriptors, so
    the Go signature is the only place they are written down. The descriptors
    are read from a local `go build -o bin/<binary> .` when there is one, and
    otherwise from the generated ``_generated_interface.py`` beside the module.
    A binary that registers several tasks needs ``task=`` to pick one; call
    ``go_task`` once per task to declare them all.

    The worker image is built from declarative layers by default and named
    ``<binary>-worker``; ``image_name`` overrides that. Pass ``dockerfile`` to
    supply your own build (see :func:`go_worker_image` for the contract), or
    ``image`` for a fully custom ``flyte.Image``.

    Pass ``reuse`` to run the task in a **warm container**: the backend keeps a
    pool of replicas alive and streams actions to them. This needs the Go side
    to opt in too — the module depends on
    ``github.com/unionai/union-reuse-go`` and main() calls ``reuse.Main()``
    instead of ``flyteruntime.Main()``. A binary built that way still runs fine
    without ``reuse``, so the two halves can be changed in either order.

    Extra keyword arguments (``retries``, ``cache``, ``resources``, ``timeout``,
    ...) pass straight through to the underlying task template.

    Returns ``(task, env)``. The env matters for composition: a Python parent
    calling this task needs ``depends_on=[env]`` so the worker image is carried
    into the deployment plan.
    """
    if reuse is not None:
        validate_reuse(reuse)

    descriptors = load_descriptors(
        module_dir=module_dir,
        search_from=module_dir,
        binary=binary,
        fallback=fallback_descriptors,
    )
    descriptor = select_descriptor(descriptors, task)

    worker_task = GoWorkerTask(
        name=descriptor["task"],
        binary=binary,
        # The task type is what routes a task to a backend plugin: `go-task`
        # lands on the default pod plugin (one pod per action), `actor` on the
        # fasttask plugin that owns replica pools.
        task_type=ACTOR_TASK_TYPE if reuse is not None else GO_TASK_TYPE,
        image=image
        or go_worker_image(
            module_dir=module_dir,
            binary=binary,
            workspace=workspace,
            dockerfile=dockerfile,
            image_name=image_name,
            reuse=reuse is not None,
            platform=platform,
        ),
        interface=native_interface(descriptor),
        reusable=reuse,
        **task_kwargs,
    )
    env = flyte.TaskEnvironment.from_task(
        env_name or f"{binary.replace('-', '_')}_env", worker_task
    )
    # `from_task` has no `reusable` parameter, and the field is what the SDK
    # reads off the *task* when serializing. Setting it on the env too keeps the
    # pair consistent for anything that inspects the environment instead.
    if reuse is not None:
        object.__setattr__(env, "reusable", reuse)
    return worker_task, env
