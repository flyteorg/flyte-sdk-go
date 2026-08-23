"""The worker image, declared as Flyte image layers.

Declarative layers instead of a Dockerfile, so this works with the **remote**
image builder (`image: {builder: remote}`): no docker on your machine. The
layer DSL is single-stage, so the Go toolchain and sources stay in the final
image (same upstream limit flyteplugins-rs lives with); the image IDL has no
ENTRYPOINT layer, so the task puts the binary path in `args[0]`.

Go workers are statically linked (`CGO_ENABLED=0`): unlike the Rust worker,
nothing at runtime needs python or shared libraries.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import flyte

# Fallback only: the tag hash can only honour an ignore file named
# `.dockerignore` sitting at an ancestor of the copied sources (see
# `_ignore_file_for`), which a file shipped inside this package never is.
_PACKAGED_IGNORE_FILE = Path(__file__).parent / "go.dockerignore"

_IGNORE_NAME = ".dockerignore"


def _ignore_file_for(context_root: Path) -> Path:
    """The ignore file to declare, preferring one the tag hash can actually use.

    `Image._get_hash_digest` loads only `<dir>/.dockerignore` and only matches
    paths underneath `<dir>`; anything else is silently a no-op for the tag, so
    the hash reads every "ignored" file — `bin/` and module caches included —
    and any local build mints a new tag and triggers a full image rebuild.
    """
    candidate = context_root / _IGNORE_NAME
    if candidate.is_file():
        return candidate

    print(
        f"warning: no {_IGNORE_NAME} at {context_root}; falling back to "
        f"{_PACKAGED_IGNORE_FILE.name}, which the SDK cannot apply when hashing the "
        f"image tag. Expect a rebuild whenever bin/ changes. Fix: copy "
        f"{_PACKAGED_IGNORE_FILE} to {candidate}.",
        file=sys.stderr,
    )
    return _PACKAGED_IGNORE_FILE


_DOCKERFILE_CONTRACT = """A worker Dockerfile has to satisfy two things (three with reuse):

0. **With `reuse=`, `unionai-actor-bridge` resolves on PATH.** A reusable task's
   pod never runs the task's own args: the fasttask plugin overwrites them with
   `unionai-actor-bridge --queue-id ... --worker-id ...`. A binary whose main()
   calls reuse.Main() handles that argv itself, so the Dockerfile only has to
   give it the name (a second COPY of the same static binary works in
   distroless/scratch stages):
   `COPY --from=build /out/<binary> /usr/local/bin/unionai-actor-bridge`
1. **The binary lands at `/usr/local/bin/<binary>`.** The task passes that path
   as `args[0]`, so it is the one hard-coded contract between image and task.
2. **No ENTRYPOINT is required.** With none set, Kubernetes execs the args
   directly. Setting one is also fine: the worker skips a leading token it does
   not recognise.

Go workers built with CGO_ENABLED=0 are static: `gcr.io/distroless/static` or
even `scratch` runtime stages work (add ca-certificates for TLS endpoints).

Custom Dockerfiles only build with the **local** docker builder
(`flyte --image-builder local run ...`), and need a registry: pass `registry=`
or set GO_IMAGE_REGISTRY.
"""


def _resolve_platform(
    platform: str | tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    """The architectures to build for, or None to keep flyte.Image's default.

    Resolution order: explicit ``platform=``, then the ``GO_IMAGE_PLATFORM``
    env var (comma-separated). The default stays flyte.Image's linux/amd64.
    """
    if platform is None:
        platform = os.environ.get("GO_IMAGE_PLATFORM")
    if platform is None:
        return None
    if isinstance(platform, str):
        platform = tuple(p.strip() for p in platform.split(",") if p.strip())
    return platform or None


def _dockerfile_image(
    *,
    dockerfile: Path,
    binary: str,
    registry: str | None,
    workspace: Path | None,
    image_name: str | None,
    reuse: bool,
    platform: tuple[str, ...] | None,
) -> flyte.Image:
    """A worker image built from a user-supplied Dockerfile."""
    if not dockerfile.is_file():
        raise FileNotFoundError(f"dockerfile {dockerfile} does not exist")
    if reuse and "unionai-actor-bridge" not in dockerfile.read_text():
        # Cheap and only a heuristic, but the failure it prevents is expensive:
        # a pool of replicas that exit instantly, with no task log to explain it.
        print(
            f"warning: {dockerfile} never mentions unionai-actor-bridge, but this task "
            f"declares reuse=. The fasttask plugin launches replicas with that command, "
            f"so the image must provide it:\n"
            f"    COPY --from=build /out/{binary} /usr/local/bin/unionai-actor-bridge",
            file=sys.stderr,
        )
    if registry is None:
        raise ValueError(
            "a registry is required to build from a Dockerfile: pass registry=, or set "
            "GO_IMAGE_REGISTRY. Unlike the layered build, flyte.Image.from_dockerfile "
            "has no registry to inherit."
        )
    if workspace is not None:
        raise ValueError(
            "workspace= and dockerfile= are mutually exclusive — a Dockerfile defines its "
            "own build context (the directory holding it), so the workspace would not be "
            "copied. Move the COPY steps into the Dockerfile."
        )
    return flyte.Image.from_dockerfile(
        file=dockerfile.resolve(),
        registry=registry,
        name=image_name or f"{binary}-worker",
        platform=platform,
    )


def _install_commands(build_dir: str, binary: str, reuse: bool) -> list[str]:
    """Build the worker and put it where the task expects to find it.

    With ``reuse``, the binary gets a second name: a reusable task's pod is
    launched with ``unionai-actor-bridge ...`` args, so that name has to
    resolve on PATH. A link suffices because the binary decides what to do from
    argv — reuse.Main() recognises a pool launch.
    """
    steps = [
        f"cd {build_dir}",
        f"CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/{binary} .",
    ]
    if reuse:
        steps.append(f"ln -sf /usr/local/bin/{binary} /usr/local/bin/unionai-actor-bridge")
    return [" && ".join(steps)]


def go_worker_image(
    *,
    module_dir: Path,
    binary: str,
    workspace: Path | None = None,
    dockerfile: Path | None = None,
    image_name: str | None = None,
    go_base: str = "golang:1-bookworm",
    registry: str | None = None,
    reuse: bool = False,
    platform: str | tuple[str, ...] | None = None,
) -> flyte.Image:
    """An image that compiles `binary` from source and installs it in /usr/local/bin.

    Normally `module_dir` is the whole build context: `go build` fetches
    dependencies from the module proxy like any other build. Pass `workspace`
    only when the module cannot build alone — replace directives pointing at
    sibling modules (a go.work monorepo). That copies the workspace root, and
    the build runs from the module's directory inside it.

    `dockerfile` replaces the declarative layers entirely, for builds the layer
    DSL cannot express — see `_DOCKERFILE_CONTRACT`.

    `reuse` adds one step to the layered build: a second name for the binary,
    so the pod the fasttask plugin launches can find it.
    """
    registry = registry if registry is not None else os.environ.get("GO_IMAGE_REGISTRY")
    resolved_platform = _resolve_platform(platform)

    if dockerfile is not None:
        return _dockerfile_image(
            dockerfile=dockerfile,
            binary=binary,
            registry=registry,
            workspace=workspace,
            image_name=image_name,
            reuse=reuse,
            platform=resolved_platform,
        )
    context_root = workspace if workspace is not None else module_dir
    ignore_file = _ignore_file_for(context_root)

    image = (
        flyte.Image.from_base(go_base)
        .clone(
            name=image_name or f"{binary}-worker",
            registry=registry,
            extendable=True,
            platform=resolved_platform,
        )
        # Replaces the SDK's default ignore set, which does not know about Go's
        # bin/ convention — without this a local build changes the image tag.
        .with_dockerignore(ignore_file)
        # The local docker builder chowns COPY layers to a `flyte` user; the
        # remote builder chowns to the base image's runtime user. Create the
        # user so one definition builds under either.
        .with_commands(["useradd --system --create-home flyte || true"])
    )

    if workspace is None:
        # Standalone-module shape: the user's module is the entire context.
        return image.with_source_folder(module_dir, "./app").with_commands(
            _install_commands("app", binary, reuse)
        )

    # Monorepo shape: replace directives reach outside the module, so the whole
    # workspace's sources have to be present.
    try:
        rel = module_dir.resolve().relative_to(workspace.resolve())
    except ValueError:
        raise ValueError(f"module_dir {module_dir} is not inside workspace {workspace}") from None
    return image.with_source_folder(workspace, "./ws").with_commands(
        _install_commands(f"ws/{rel}", binary, reuse)
    )
