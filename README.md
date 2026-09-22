# Async Task Pipeline

[![PyPI](https://img.shields.io/pypi/v/async-task-pipeline-py?cacheSeconds=1800)](https://pypi.org/project/async-task-pipeline-py/)
[![Python](https://img.shields.io/pypi/pyversions/async-task-pipeline-py?cacheSeconds=1800)](https://pypi.org/project/async-task-pipeline-py/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://github.com/a-buryy/async-task-pipeline-py/blob/main/LICENSE)
[![CI](https://github.com/a-buryy/async-task-pipeline-py/actions/workflows/ci.yml/badge.svg)](https://github.com/a-buryy/async-task-pipeline-py/actions/workflows/ci.yml)

`async-task-pipeline-py` runs async tasks with dependencies inside a `TaskGroup`-style scope: open the pipeline with `async with`, call `start()` where you would call `create_task()`, and leaving the block waits for every task. On top of `TaskGroup` it adds:

*   **Dependencies**: pass a task's handle as an argument, and the callable receives that task's result.
*   **A concurrency limit** on how many tasks run at once.
*   **Scoped failures**: a failure is held for the task that started it instead of cancelling everything. Only an unhandled failure ends the pipeline, as an `ExceptionGroup`.

**When to use it.** `asyncio.gather` and `TaskGroup` are enough for a flat batch of independent coroutines. Reach for a pipeline when tasks feed each other's results, when a fan-out has to be throttled, or when a task discovers more work while it runs.

## Contents

*   [Installation](#installation)
*   [Quick start](#quick-start)
*   [Guides](#guides)
    *   [Wiring individual tasks](#wiring-individual-tasks)
    *   [Dynamic tasks](#dynamic-tasks)
    *   [Failure semantics](#failure-semantics)
    *   [Fanning out over an async generator](#fanning-out-over-an-async-generator)
    *   [Extra arguments for a fan-out](#extra-arguments-for-a-fan-out)
    *   [Logging](#logging)
*   [API reference](#api-reference)
*   [Development](#development)

## Installation

```bash
pip install async-task-pipeline-py
```

The distribution is named `async-task-pipeline-py`; the import package is `async_task_pipeline`. Requires Python 3.12+ and has no runtime dependencies.

## Quick start

```python
import asyncio

from async_task_pipeline import TaskPipeline


async def fetch(number: int) -> int:
    await asyncio.sleep(0.1)
    return number * 10


async def total(values: list[int]) -> int:
    return sum(values)


async def main() -> None:
    async with TaskPipeline(concurrency_limit=3) as pipeline:
        # five tasks, at most three at once
        values = pipeline.start_many(fetch, range(5), task_name='fetch')
        # runs once every fetch is done
        summary = pipeline.start(total, values, task_name='total')

    print(summary.result())
    print(pipeline.results())


asyncio.run(main())
```

```
100
{'fetch[0]': 0, 'fetch[1]': 10, 'fetch[2]': 20, 'fetch[3]': 30, 'fetch[4]': 40, 'fetch': [0, 10, 20, 30, 40], 'total': 100}
```

`start()` and `start_many()` return immediately with a `TaskHandle`. Passing `values` to `start(total, ...)` makes `total` depend on it: the pipeline replaces the handle with its result, a list here, before calling `total`. Leaving the `async with` block waits for everything, after which `result()` reads a task's value and `results()` returns all of them by task name.

To wait for tasks whose results you don't need, list them in `depends_on`:

```python
pipeline.start(cleanup, depends_on=[values, summary])
```

## Guides

The snippets below assume these imports:

```python
import asyncio
import functools
from collections.abc import AsyncIterator

from async_task_pipeline import (
    TaskHandle,
    TaskPipeline,
    current_pipeline,
)
```

### Wiring individual tasks

`start_many` hands back a single handle for the whole fan-out, so anything consuming it waits for every item. When a downstream task needs only *one* of the results, start the tasks individually and keep their handles:

```python
async with TaskPipeline(concurrency_limit=5) as pipeline:
    shards = [pipeline.start(fetch_shard, index, task_name=f'shard{index}') for index in range(5)]

    # Depends on one shard, so it runs as soon as that shard is ready.
    inspected = pipeline.start(inspect_shard, shards[4], task_name='inspect')

    # Depends on all of them.
    summary = pipeline.start(summarise, *shards, task_name='summary')
```

`inspect` starts the moment shard 4 lands, rather than waiting for the slowest shard:

```
fetched 4, inspected 4, fetched 3, fetched 2, fetched 1, fetched 0
```

This changes *when* work starts, not what happens on failure: a failure nobody holds still ends the pipeline, however the dependency is wired.

If a consumer wants the results as one list rather than as separate arguments, collect them with `gather()`. It returns a `TaskHandle[list[T]]` in argument order, the same shape `start_many` produces:

```python
async with TaskPipeline() as pipeline:
    shards = [pipeline.start(fetch_shard, index) for index in range(5)]
    everything = pipeline.gather(*shards)
    summary = pipeline.start(summarise, everything, task_name='summary')
```

### Dynamic tasks

A running task starts further tasks the same way, reaching its pipeline with `current_pipeline()`. Use `await handle.wait()` when the task needs a result to decide what to do next. The waiting task gives up its concurrency slot while it waits, so this is safe even when the limit is smaller than the number of waiters.

```python
async def crawl() -> list[str]:
    pipeline = current_pipeline()
    found: list[str] = []
    cursor = 0

    while True:
        # Started together so they run concurrently, then awaited one by one.
        batch = [pipeline.start(fetch_page, cursor + offset) for offset in range(3)]
        pages = [await page.wait() for page in batch]

        found.extend(page for page in pages if page is not None)

        if None in pages:
            return found

        cursor += 3
```

When the task doesn't need the results itself, don't wait at all. Start a successor that consumes them and return, which frees the slot immediately:

```python
async def parent() -> str:
    pipeline = current_pipeline()
    children = [pipeline.start(work, index) for index in range(4)]

    pipeline.start(finalize, *children)

    return 'spawned'
```

### Failure semantics

A task that starts another can handle its failure, and the pipeline carries on:

```python
async def fetch(url: str) -> str:
    if 'bad' in url:
        raise ValueError(f'cannot fetch {url}')
    return f'<page {url}>'


async def fetch_or_default(url: str) -> str:
    page = current_pipeline().start(fetch, url)

    try:
        return await page.wait()
    except ValueError:
        return '<empty>'


async with TaskPipeline() as pipeline:
    pages = pipeline.start_many(fetch_or_default, ['a', 'bad', 'c'])

print(pages.result())  # ['<page a>', '<empty>', '<page c>']
```

Calling `pipeline.start_many(fetch, ...)` directly from the `async with` body instead leaves nobody to handle the failure, so the pipeline ends and the `async with` raises an `ExceptionGroup` containing the `ValueError`.

The rule behind this: every task has an **owner**, the task that called `start()` for it, or nobody when it was started from the scope body. The owner is answerable for the failure, and the rule does not depend on timing. Unlike `TaskGroup`, a single failure does not cancel sibling tasks while its owner is running:

*   While the owner is running, a failure is **held**. It is delivered to whoever reads it, through `await handle.wait()` or `handle.result()`, from the owner or any other task, and the pipeline carries on. That makes `try`/`except` around `await handle.wait()` meaningful whether the wait begins before or after the failure.
*   If the owner finishes without anyone having read the failure, the owner **fails with it**, and the same rule applies one level up. Nothing is lost silently.
*   If the owner finishes while a task it started is still running, that task **passes to the owner's own owner**. A failure is always held by the nearest running ancestor, however late it lands.
*   A failure with **no owner to hold it**, from a task started from the scope body or one whose every ancestor has finished, ends the pipeline at once and surfaces from the `async with` as an `ExceptionGroup`.
*   Tasks downstream of a failure are discarded; their `result()` raises `RuntimeError` explaining why.
*   Every failure carries notes naming the task it came from and, if it was left unhandled, the owner that dropped it, so a callable used by several tasks is still identifiable:

```
ValueError: bad payload
Raised by pipeline task 'shards[3]'.
Not handled by its owner, pipeline task 'collect'.
```

Waiting cannot deadlock on the concurrency limit, because a waiting task gives its slot back. A *cycle* of waits would still be unfinishable: a task waiting on itself, two waiting on each other, or one waiting on a task that cannot start until the waiter finishes. `wait()` checks for that before blocking and raises instead:

```
RuntimeError: Task 'parent' cannot wait on task 'later': that would deadlock,
because 'later' is itself blocked on 'parent'.
```

To bound a pipeline that might hang for any other reason, wrap it in `asyncio.timeout`:

```python
async with asyncio.timeout(30):
    async with TaskPipeline() as pipeline:
        ...
```

### Fanning out over an async generator

`start_many` needs a synchronous iterable. It calls `list(inputs)`, so an async generator raises `TypeError: 'async_generator' object is not iterable`. Iterate it yourself and collect the handles with `gather()`:

```python
async def pages() -> AsyncIterator[int]:
    ...


async with TaskPipeline() as pipeline:
    handles = [pipeline.start(fetch, page) async for page in pages()]
    everything = pipeline.gather(*handles)
```

Each task starts as its item arrives, rather than after the whole sequence is produced. Note that the `async for` makes the scope body yield to the event loop, so tasks begin running while the body is still executing, which matters if your code assumes nothing runs until the body ends.

`gather(*handles)` is evaluated eagerly, so this drains the generator completely before the combined handle exists. For a source of unknown length, or when downstream tasks need the handle before the items have arrived, do the draining inside a task instead:

```python
async def collect() -> list[int]:
    pipeline = current_pipeline()
    handles = [pipeline.start(fetch, page) async for page in pages()]
    return [await handle.wait() for handle in handles]


async with TaskPipeline() as pipeline:
    collected = pipeline.start(collect, task_name='collect')
    total = pipeline.start(summarise, collected, task_name='total')
```

`collected` exists immediately, so `total` can be wired before a single page has been fetched. `collect` gives up its slot on each `wait()`, so it doesn't occupy one while the tasks it started are running.

### Extra arguments for a fan-out

`start_many` always calls `fn(item)`, so arguments that are the same for every sub-task go on the callable with `functools.partial`, by keyword or as a positional prefix:

```python
pipeline.start_many(functools.partial(fetch, retries=3), urls, task_name='fetch')
pipeline.start_many(functools.partial(prefixed, 'item-'), values, task_name='prefixed')
```

Arguments that differ per sub-task belong in the item itself (a tuple, a dataclass or a dict) rather than in a partial.

A `TaskHandle` bound into a partial is **not** replaced by its result, and no dependency is recorded: only the direct arguments of `start()` and `start_many()` are inspected, and a partial is a container like any other. The callable receives the handle object itself. That object is still usable, and you have three options:

**Resolve it a level up**, where it is a direct argument. The sub-tasks then depend on it, and an upstream failure discards them before they run:

```python
async def fan_out(config: Config) -> list[str]:
    pipeline = current_pipeline()
    return await pipeline.start_many(functools.partial(fetch, config=config), urls).wait()


pipeline.start(fan_out, config_handle)
```

**Declare the dependency the partial hid**, and read the result directly. The sub-tasks then wait for it, so `result()` is safe:

```python
pipeline.start_many(functools.partial(fetch, config=config_handle), urls, depends_on=[config_handle])
```

Without that `depends_on`, nothing orders the two tasks, and `result()` raises `RuntimeError: Task '...' has not completed.` whenever the upstream task hasn't happened to finish first. Note that the handle is now named twice and nothing checks the two agree: bind one and list another, and you silently get ordering against the wrong task.

**Await it inside the callable.** `await handle.wait()` works on a bound handle exactly as it does anywhere else. No `depends_on` is needed, because the wait does the ordering:

```python
async def fetch(url: str, *, config_handle: TaskHandle[Config]) -> str:
    config = await config_handle.wait()
    ...
```

The three differ on failure, so pick deliberately. The first two make the task a *dependent*: an upstream failure discards it. Awaiting makes the task an *awaiter*: the exception is delivered to it, and `try`/`except` works. Whether the pipeline carries on is decided by who *owns* the failing task (see [Failure semantics](#failure-semantics)). Start the family from inside one task and the failure is held for that task while its awaiters deal with it; start it from the scope body and there is nobody to hold it, so the pipeline ends.

### Logging

The library logs its lifecycle at `DEBUG` on the `async_task_pipeline.pipeline` logger: each task started with its dependency count, each task's start and wall-clock duration, failures, discards and a closing tally. It attaches a `NullHandler`, so nothing is emitted until you configure logging yourself.

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```

```
Pipeline opened with concurrency limit 2.
Started task 'items[0]' with 0 pending dependencies.
Started task 'items' with 2 pending dependencies.
Task 'items[0]' started.
Task 'items[0]' finished in 0.010 s.
Task 'failing' failed: ValueError('bad payload')
Pipeline closed: 3 of 4 tasks completed.
```

## API reference

### `TaskPipeline(concurrency_limit=10)`

An async context manager. `concurrency_limit` bounds how many tasks *execute* at once; a task inside `wait()` does not count against it. A pipeline can be opened once, and starting a task outside its scope raises `RuntimeError`.

| Method | Returns | Description |
|---|---|---|
| `start(fn, *args, task_name=None, depends_on=(), **kwargs)` | `TaskHandle[T]` | Start `fn(*args, **kwargs)` once its dependencies have resolved. Every `TaskHandle` passed directly in `args` or `kwargs` is a dependency and is replaced by its result; handles nested in lists, dicts or partials are not. |
| `start_many(fn, inputs, *, task_name=None, depends_on=())` | `TaskHandle[list[T]]` | Start `fn(item)` for each item in `inputs`. The result is the list of results, in input order. |
| `gather(*handles, task_name=None)` | `TaskHandle[list[T]]` | Combine individually started tasks into one handle whose result is the list of theirs, in argument order. |
| `results()` | `dict[str, Any]` | Every completed task's result, keyed by task name. Callable at any time: partial while running, and after a failure it still holds whatever was produced. |

`depends_on` lists tasks to wait for without receiving their results.

**Task names** are optional. A name defaults to the callable's `__name__` (looking through `functools.partial`), made unique with a `#N` suffix, and `start_many` names its sub-tasks `f'{task_name}[{i}]'`. An unnamed fan-out over `fetch` therefore reads `fetch`, `fetch[0]`, `fetch[1]`.

### `TaskHandle[T]`

Returned by every `start` call, and generic in the callable's return type: `start(fetch)` on an `async def fetch(...) -> int` gives a `TaskHandle[int]`. Pass it as an argument to another task to wire a dependency.

| Member | Description |
|---|---|
| `result()` | The task's return value. Re-raises the task's exception if it failed. Raises `RuntimeError` if the task has not completed, was cancelled, or was discarded because a dependency did not complete. |
| `await wait()` | Wait for the result from inside a running task, handing the concurrency slot back meanwhile. Raises the task's exception if it failed. |
| `done()` | Whether the task has settled: with a result, a failure or a discard. |
| `name` | The task's unique name, as used in `results()`. |

### `current_pipeline()`

Returns the running pipeline from inside a task, so a task can start further work without being passed the pipeline. Raises `RuntimeError` if no pipeline is open in the current task.

## Development

This project was developed with the help of Claude (Anthropic).

The project uses [uv](https://docs.astral.sh/uv/). Set up the environment and the pre-commit hooks with:

```bash
uv sync
uv run pre-commit install
```

*   **Run tests**: `uv run pytest tests/`
*   **Run linter**: `uv run ruff check .`
*   **Run formatter**: `uv run ruff format --check .`
*   **Run type checker**: `uv run mypy .`

## License

Released under the MIT License. The full text is in the `LICENSE` file.
