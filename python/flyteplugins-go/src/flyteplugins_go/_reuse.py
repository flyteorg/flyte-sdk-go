"""Declaring a Go worker as a reusable (warm) container.

A reusable task stops getting a pod per action. The backend keeps a pool of
replicas alive and hands them work over a gRPC heartbeat stream — the
fasttask/"actor" plugin. Two things have to be true for a Go task to join that
pool:

1. **The task template routes there.** The plugin registers for task types
   ``actor`` and ``fast-task``, and reads the environment's shape out of
   ``TaskTemplate.custom``. Python's SDK writes that blob only for its own
   ``"python"`` task type, so we set the type and build the blob here.
2. **The image answers to ``unionai-actor-bridge``.** The plugin replaces the
   container's args with that command, so the name has to exist on PATH. See
   ``_image.go_worker_image``, which links the worker binary to it.

The Go side of the contract is the ``github.com/unionai/union-reuse-go``
module: swap ``flyteruntime.Main()`` for ``reuse.Main()`` and the binary learns
to hold a lease and run several actions at once. Nothing in this module depends
on it — the two halves meet at the task template and the image, not in code.

This file is a copy of flyteplugins-rs's ``_reuse.py`` modulo names — the two
must stay in lockstep, since a divergent identity hash would put Go tasks in a
pool that agrees with nothing else.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import flyte
    from flyte.models import SerializationContext

# The plugin's own name for the task type. `fast-task` routes identically;
# `actor` is what the Python SDK emits, so it is the better-trodden path.
ACTOR_TASK_TYPE = "actor"


class MissingReuseSupport(Exception):
    """The installed `flyte` does not expose the internals this needs.

    Raised at declaration time rather than at launch, because the alternative is
    a task that registers cleanly and then produces replicas the plugin never
    schedules onto.
    """


def _reuse_internals():
    """The two private helpers we borrow from the Python SDK.

    Borrowing rather than reimplementing is deliberate: ``extract_unique_id_and_image``
    computes the environment's *identity* — replicas are shared by every task
    whose hash matches, so a hash of our own would put Go tasks in a pool that
    agrees with nothing else and drift from the SDK's rules about what forces a
    new pool (image, resources, secrets, TTLs).
    """
    try:
        from flyte._internal.runtime.reuse import extract_unique_id_and_image
        from flyte._internal.runtime.task_serde import _get_urun_container
    except ImportError as e:  # pragma: no cover - depends on the installed flyte
        raise MissingReuseSupport(
            "this version of `flyte` does not expose the reuse internals "
            "flyteplugins-go builds the actor environment from "
            f"({e}). Pin a compatible flyte, or drop `reuse=` to run the task "
            "as an ordinary one-shot container."
        ) from e
    return extract_unique_id_and_image, _get_urun_container


def actor_custom_config(
    task: Any,
    sctx: SerializationContext,
    reuse: flyte.ReusePolicy,
) -> dict[str, Any]:
    """The ``ExecutionEnv`` blob the fasttask plugin unmarshals from ``custom``.

    Mirrors ``flyte._internal.runtime.reuse.add_reusable``. Field names are the
    plugin's proto (``FastTaskEnvironmentSpec``), so they are not ours to rename.
    """
    extract_unique_id_and_image, get_container = _reuse_internals()

    from flyteidl2.core import tasks_pb2

    env_name = task.parent_env_name or task.name.split(".")[0]

    # The identity hash reads the container with `args` cleared, which is exactly
    # what lets one pool serve many actions: args carry the per-action input and
    # output paths, and hashing them would mint a replica per action.
    probe = tasks_pb2.TaskTemplate(container=get_container(sctx, task))
    version, image_uri = extract_unique_id_and_image(
        env_name=env_name,
        code_bundle=sctx.code_bundle,
        task=probe,
        reuse_policy=reuse,
    )

    scaledown_ttl = reuse.get_scaledown_ttl()
    return {
        "name": env_name,
        # The plugin builds pod names from this and has 63 characters to work
        # with; the SDK truncates to 15 and so must we, or the two disagree
        # about which environment a replica belongs to.
        "version": version[:15],
        "type": "actor",
        "spec": {
            "container_image": image_uri,
            # v2 workers report no backlog capacity, so a length here would only
            # queue work on a replica that will never admit to holding it.
            "backlog_length": None,
            "parallelism": reuse.concurrency,
            "min_replica_count": reuse.min_replicas,
            "replica_count": reuse.max_replicas,
            "ttl_seconds": reuse.idle_ttl.total_seconds() if reuse.idle_ttl else None,
            "scaledown_ttl_seconds": scaledown_ttl.total_seconds() if scaledown_ttl else None,
        },
    }


def validate(reuse: flyte.ReusePolicy) -> None:
    """Reject policies the Go worker cannot honour.

    `ReusePolicy.__post_init__` has already normalized and range-checked
    everything. Go tasks run each action on its own goroutine, so any
    concurrency >= 1 is fine.
    """
    if reuse.concurrency < 1:
        raise ValueError(f"concurrency must be at least 1, got {reuse.concurrency}")
