# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses `uv` (Python 3.12+) for dependency management. All dev tools run through `uv run`.

- Run tests: `uv run pytest tests/`
- Run a single test: `uv run pytest tests/test_pipeline.py::test_name`
- Lint: `uv run ruff check .`
- Format check: `uv run ruff format --check .`
- Format (apply): `uv run ruff format .`
- Type check: `uv run mypy .`

`pytest` is configured with `asyncio_mode = "auto"` — `async def` tests run without an `@pytest.mark.asyncio` decorator.

Pre-commit hooks (`.pre-commit-config.yaml`) run the locked tools through `uv run`: ruff check and ruff format --check on staged Python files, and mypy on the whole project.

mypy runs with `strict = true`, relaxed for `tests.*` (untyped test helpers are allowed, their bodies still checked). That override matches only because `tests/__init__.py` exists — keep the empty file.

## Compatibility and packaging

- **Python 3.12–3.14.** 3.12 is the floor because of PEP 695 generics (`class TaskHandle[T]`, `def start[T]`); mypy targets 3.12 so newer typing features are flagged. `.python-version` picks the local dev interpreter only. Check other versions with `uv run --python 3.12 --isolated pytest tests/` (likewise 3.13, 3.14) — asyncio behaviour differs between them in ways static checks can't see (see the `_spawn` gotcha).
- **Names.** The distribution is `async-task-pipeline-py`; the import package is `async_task_pipeline`. Built with hatchling, which is pointed at the package explicitly because the names differ.
- **Installed, not path-hacked.** `__version__` comes from `importlib.metadata`, so the package must be installed; `uv sync` does an editable install, and tests import it that way rather than through a `sys.path` tweak.
- **Public API.** `__all__` in `__init__.py` is the public surface. `py.typed` ships the annotations to users, so public signatures are part of the API too.
- **Keep the docs in step.** A public API or behaviour change also updates the README (its API reference tables and guides), the module docstring where it summarises behaviour, and `CHANGELOG.md` under `[Unreleased]` (Keep a Changelog, SemVer; version lives in `pyproject.toml`).

## Architecture

The whole library lives in one module, `async_task_pipeline/pipeline.py`. It exposes three public names:

**`TaskPipeline`** — an async context manager, and the scope a workflow lives in. There is no build phase: `start()` schedules immediately and behaves identically in the `async with` body and inside a running task, so there is only one submission path to reason about.

- `start(fn, *args, task_name=None, depends_on=(), **kwargs)`: register and schedule one async callable. Any top-level `TaskHandle` in `args`/`kwargs.values()` is a dependency, substituted for its result at call time. Handles nested in containers are deliberately *not* inspected — both scans (`_collect_dependencies` at start time, `_resolve` at call time) are top-level only and must stay in agreement.
- `start_many(fn, inputs, *, task_name=None, depends_on=())`: one sub-task per input plus a `_collect_list` barrier whose result is the list. Names the barrier first, then hangs sub-tasks off it as `f'{task_name}[{i}]'`, and validates the whole family before starting anything — `start()` runs work immediately, so a rejection halfway through would leave real tasks in flight. A handle given as an *item* is resolved: each item is a direct argument to its sub-task, so the top-level rule covers it.
- `gather(*handles, task_name=None)`: the `_collect_list` barrier on its own, for tasks started individually rather than through `start_many`. Names itself `gather` via `_derive_name(..., base=...)` so the private coroutine's name never leaks into `results()`.
- `results()`: a fresh snapshot keyed by task name. A method, not a property, because it walks the graph on every call.
- `__aexit__` keeps the `TaskGroup` open across the drain, because tasks finishing there still release dependents that must be spawnable.

**`TaskHandle[T]`** — generic in its task's result type. Owns its own `asyncio.Future` and holds *no* reference to the pipeline, so keeping one handle doesn't pin the pipeline or every other task's result.

- `result()` — a method, not a property, because it raises three different ways (not completed, discarded, or the task's own exception). Mirrors `asyncio.Future.result()`.
- `await handle.wait()` — waits from inside a running task, releasing the permit first. Deliberately not `__await__`: making the handle awaitable meant every discarded `start()` call looked like a forgotten `await` to IDEs.

**`current_pipeline()`** — ambient lookup via ContextVar, so a task can start more work without being handed the pipeline. There are intentionally no module-level `start()`/`start_many()` wrappers; they were pure delegation.

**Scheduling.** Kahn-style dependency counting, not a future-await per node. `_Node` carries `pending_dependencies` and a `dependents` list; a node becomes an asyncio task only when its last dependency completes (`_release_dependents` → `_spawn`). A long dependency chain therefore holds one live task at a time, not one per node.

**The permit, and why waiting is safe.** `concurrency_limit` is an `asyncio.Semaphore`, but `_execute` holds it through a `_Permit` object rather than `async with`. `wait()` releases the permit before blocking and re-claims it after, so a task waiting on a child never starves the child — the deadlock that a plain "hold the slot for the whole callable" design has whenever blocked parents ≥ the limit. `_Permit.acquire`/`release` are idempotent so that a *cancelled* re-acquire leaves the permit un-held and the outer `finally` doesn't release it twice, which would silently inflate the limit.

**Wait cycles are detected, not hung.** Releasing the permit removes the *resource* deadlock, but a cycle in the wait graph is still unfinishable. Before blocking, `wait()` calls `_guard_no_wait_cycle`, which searches what the awaited task is itself blocked on and raises if it reaches the waiter. It follows one edge kind, `TaskHandle._blocked_on`: a tuple holding the handle a task is inside `wait()` on, or the dependencies it has not yet received (recorded synchronously in `start()`, cleared in `_spawn`/`_discard`). A `depends_on` edge alone can't cycle, but combined with a wait it can, and only the dependency half is in the graph — so the dependency edges have to be on the handle for the walk, not just in the node's pending count.

The `_Permit` held in the `_current_permit` ContextVar also records the owning `asyncio.Task` (`_Permit.task`) and the handle it was claimed for (`_Permit.handle`), so a waiter knows who it is. A raw `create_task` inside a node inherits the node's context, so `wait()` checks task identity to stop it handing back a permit it doesn't own.

**Failure semantics are decided by ownership, not by who is waiting.** `start()` passes the calling pipeline task as the new handle's `owner` (`None` from the scope body), and the handle registers itself in the owner's `_owned` list. In `_execute`, a failing task sets the exception on its future and checks `handle._owner`:

- owner present → held. The exception is swallowed here; anyone reading it via `wait()`/`result()` sets `_failure_retrieved`. The pipeline continues.
- no owner → re-raised, which aborts the `TaskGroup` and surfaces as an `ExceptionGroup`.

The owner is always a *running* task, by construction: whenever a task finishes (return, failure or cancellation) `_execute` calls `handle._hand_over_owned()`, which walks `_owned` once. Children still running are re-parented to the finishing task's own `_owner` (and appended to its `_owned`), so a late failure walks up the chain instead of falling out of it; children that failed unread come back as `(handle, exception)` pairs. That walk happens *before* the finishing task's future is settled, and nothing awaits in between, so there is no window in which an owner is done but still owns something. This is why no `owner.done()` check exists, and why the rule has no timing clause.

On the success path the first unread failure is raised *inside* the `try`, so the single `except Exception` path handles it, adding a "Not handled by its owner" note instead of a "Raised by" note (identity check against `unhandled[0][1]`). A task that fails under an owner other than the one that started it — re-parented, or passed all the way to the body — gets a "was started by ... which had already finished" note at the moment it fails (`_started_by` keeps the original owner's name for exactly this), so the note is present whether the failure is later held, dropped, or aborts the pipeline outright. Any further unread failures, and those a failing owner leaves behind, become "also left unhandled" notes. A failing owner's running children are re-parented, not cancelled. The deliberate consequence of all this is that a task started from the body cannot have its failure handled by a sibling — start the family from one task if that is wanted.

Either way `_discard_dependents` cancels the whole downstream cone, so nothing is left pending and `result()` can say "was discarded" rather than "has not completed". Dependencies are awaited via `asyncio.wait`, never awaited directly, so a cancelled waiter can't cancel the producer's future and a failure is reported exactly once.

**Why no `validate()` / cycle detection.** Dependencies must be `TaskHandle` instances issued by *this* pipeline (`_collect_dependencies` rejects foreign handles). A handle can only reference an already-started task, so cycles are structurally impossible.

**Naming.** `_derive_name` works out a name without claiming it; `_claim_name` takes it. The split exists so `start_many` can settle the barrier's name before building its sub-task names from it. Auto names come from `fn.__name__` with a `#N` uniqueness suffix; sub-tasks use `[i]` so the two roles never collide (`fetch#1[0]` is unambiguous). `_unwrap_callable` looks through `functools.partial` layers first, since a partial has no `__name__` and would otherwise name every such task `partial`; `_execute` uses it too, so the non-awaitable `TypeError` names the wrapped function.

**Gotchas worth keeping in mind**

- `_spawn` catches `RuntimeError` from `create_task`: a task that swallows its cancellation still completes and releases dependents while the group is unwinding, and a refused spawn would otherwise join the group as a second, unrelated error. The coroutine is created *before* `create_task` and closed in the `except`: Python 3.12's `TaskGroup` doesn't close a refused coroutine (3.13+ does), so inlining `self._execute(node)` into the call brings back a "coroutine was never awaited" warning on 3.12.
- `_execute` calls `future.exception()` after `set_exception` to mark it retrieved — otherwise asyncio logs a bogus "never retrieved" for a failure that is being handled.
- `getattr(fn, '__name__', ...)` needs an explicit `: str` annotation; PyCharm mis-resolves typeshed's `getattr` overloads and reports an unhashable dict key without it.
- `start()` arguments are `*args: Any` and therefore unchecked. Python's type system cannot map `X → X | TaskHandle[X]` across a `ParamSpec`, so there is no universal way to type the wiring; `await handle.wait()` is the fully-typed alternative for a specific call site.

## Tests

- `tests/test_pipeline.py` — the test suite for the public API.
- Each test's docstring states the scenario as `Case:` and the expected outcome as `Expect:`; follow that for new tests.
- Tests exercise the installed package through its public names; private attributes are only touched where a test targets an internal guarantee.

## Code style

Enforced by ruff (line length 120). Notable choices configured in `pyproject.toml`:

- Single quotes for inline strings, double quotes for docstrings (Google convention).
- `flake8-boolean-trap` (`FBT`) is enabled — boolean positional args will be flagged; use keyword-only.
- `eradicate` (`ERA`) and `flake8-print` (`T20`) are enabled — no commented-out code, no stray `print`.
- isort: `lines-after-imports = 2`, `split-on-trailing-comma = true`, `combine-as-imports = true`.
- Tests live under `tests/test_*.py` and have `S101` (assert) and `D205` relaxed.
