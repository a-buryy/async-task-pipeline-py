"""
Provide tests.
"""

import asyncio
import contextvars
import functools
import gc
import weakref
from collections.abc import (
    Awaitable,
    Callable,
)
from typing import Any

import pytest

from async_task_pipeline import (
    TaskHandle,
    TaskPipeline,
    current_pipeline,
)


async def double(value: int) -> int:
    """
    Return twice the value.
    """
    return value * 2


async def boom() -> None:
    """
    Fail with a recognisable error.
    """
    raise ValueError('boom')


def single_error[ErrorType: BaseException](
    exc_info: pytest.ExceptionInfo[ExceptionGroup[Any]],
    expected: type[ErrorType],
) -> ErrorType:
    """
    Unwrap the one error a failed pipeline is expected to have raised.
    """
    errors = exc_info.value.exceptions
    assert len(errors) == 1
    assert isinstance(errors[0], expected)
    return errors[0]


def concurrency_tracker() -> tuple[Callable[[int], Awaitable[int]], Callable[[], int]]:
    """
    Build a task that records how many copies of itself execute at once, and a reader for the peak.
    """
    live = 0
    peak = 0

    async def task(value: int) -> int:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)
        live -= 1
        return value

    return task, lambda: peak


async def test_pipeline_runs_a_single_task():
    """
    Case: start one task inside the pipeline scope.
    Expect: it runs, and its result is readable after the scope closes.
    """

    async def task() -> int:
        return 42

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(task)

    assert handle.result() == 42
    assert pipeline.results() == {'task': 42}


async def test_pipeline_substitutes_handles_in_args_and_kwargs():
    """
    Case: pass handles positionally and by keyword to a downstream task.
    Expect: each handle is replaced by its task's result before the callable runs.
    """

    async def combine(first: int, *, second: int) -> int:
        return first + second

    async with TaskPipeline() as pipeline:
        a = pipeline.start(double, 3, task_name='a')
        b = pipeline.start(double, 4, task_name='b')
        combined = pipeline.start(combine, a, second=b, task_name='combined')

    assert combined.result() == 14


async def test_pipeline_waits_on_depends_on_without_consuming():
    """
    Case: declare a dependency that the downstream task does not consume.
    Expect: the downstream task runs only after the upstream one.
    """
    order: list[str] = []

    async def first() -> None:
        order.append('first')

    async def second() -> None:
        order.append('second')

    async with TaskPipeline(max_concurrency=4) as pipeline:
        upstream = pipeline.start(first)
        pipeline.start(second, depends_on=[upstream])

    assert order == ['first', 'second']


async def test_pipeline_resolves_a_diamond_once_per_task():
    """
    Case: two tasks consume one upstream task, and a fourth consumes both.
    Expect: the shared task runs once and every result flows through.
    """
    runs = 0

    async def source() -> int:
        nonlocal runs
        runs += 1
        return 5

    async def join(left: int, right: int) -> int:
        return left + right

    async with TaskPipeline() as pipeline:
        shared = pipeline.start(source)
        left = pipeline.start(double, shared, task_name='left')
        right = pipeline.start(double, shared, task_name='right')
        tip = pipeline.start(join, left, right, task_name='tip')

    assert runs == 1
    assert tip.result() == 20


async def test_pipeline_resolves_a_diamond_of_fan_outs():
    """
    Case: four fan-out groups in a diamond — A -> {B, C} -> D — wired by their barrier handles.
    Expect: each group waits for every task of the groups it depends on, not just the first.
    """
    execution_order: list[str] = []

    async def task(task_id: str) -> str:
        execution_order.append(task_id)

        return task_id

    async with TaskPipeline() as pipeline:
        a = pipeline.start_many(task, ['A1', 'A2'], task_name='A')
        b = pipeline.start_many(task, ['B1'], task_name='B', depends_on=[a])
        c = pipeline.start_many(task, ['C1'], task_name='C', depends_on=[a])
        d = pipeline.start_many(task, ['D1'], task_name='D', depends_on=[b, c])

    assert sorted(execution_order[:2]) == ['A1', 'A2']
    assert sorted(execution_order[2:4]) == ['B1', 'C1']
    assert execution_order[4] == 'D1'
    assert d.result() == ['D1']


async def test_pipeline_fans_out_with_start_many():
    """
    Case: fan a callable out over inputs.
    Expect: the barrier handle carries the results in input order.
    """
    async with TaskPipeline() as pipeline:
        items = pipeline.start_many(double, [1, 2, 3], task_name='items')

    assert items.result() == [2, 4, 6]
    assert pipeline.results()['items[1]'] == 4


async def test_pipeline_chains_a_fan_out_into_a_consumer():
    """
    Case: pass a barrier handle to a downstream task.
    Expect: the consumer receives the list of sub-task results.
    """

    async def total(values: list[int]) -> int:
        return sum(values)

    async with TaskPipeline() as pipeline:
        items = pipeline.start_many(double, [1, 2, 3])
        summed = pipeline.start(total, items, task_name='summed')

    assert summed.result() == 12


async def test_pipeline_start_many_resolves_handles_given_as_items():
    """
    Case: fan out over a mix of plain values and handles of tasks still running.
    Expect: each handle item is a dependency of its own sub-task and arrives resolved, in input order.
    """

    async def slow(value: int) -> int:
        await asyncio.sleep(0.02)
        return value

    async with TaskPipeline() as pipeline:
        first = pipeline.start(slow, 1, task_name='first')
        second = pipeline.start(slow, 2, task_name='second')
        doubled = pipeline.start_many(double, [first, 10, second], task_name='doubled')  # type: ignore[arg-type]

    assert doubled.result() == [2, 20, 4]


async def test_pipeline_start_many_accepts_a_generator_for_depends_on():
    """
    Case: pass `depends_on` to `start_many` as a one-shot generator.
    Expect: every sub-task waits on it, not just the first one to consume the iterator.
    """
    order: list[str] = []

    async def slow() -> None:
        await asyncio.sleep(0.02)
        order.append('slow')

    async def fast(index: int) -> None:
        order.append(f'fast{index}')

    async with TaskPipeline() as pipeline:
        upstream = pipeline.start(slow)
        pipeline.start_many(fast, [0, 1], depends_on=(handle for handle in [upstream]))

    assert order == ['slow', 'fast0', 'fast1']


async def test_pipeline_start_many_takes_extra_arguments_through_a_partial():
    """
    Case: fan out a callable that needs arguments beyond the item, bound with `functools.partial`.
    Expect: every sub-task receives them, whether bound by keyword or as a positional prefix.
    """

    async def fetch(url: str, *, retries: int = 1) -> str:
        return f'{url}@{retries}'

    async def prefixed(prefix: str, value: int) -> str:
        return f'{prefix}{value}'

    async with TaskPipeline() as pipeline:
        by_keyword = pipeline.start_many(functools.partial(fetch, retries=3), ['a', 'b'], task_name='keyword')
        by_position = pipeline.start_many(functools.partial(prefixed, 'item-'), [1, 2], task_name='position')

    assert by_keyword.result() == ['a@3', 'b@3']
    assert by_position.result() == ['item-1', 'item-2']


async def test_pipeline_names_a_partial_after_the_callable_underneath():
    """
    Case: start partials without naming them, including a nested one and one wrapping an object.
    Expect: names come from the function underneath, not from `partial`.
    """

    class Fetcher:
        async def __call__(self, url: str) -> str:
            return url

    async def fetch(url: str, *, retries: int = 1) -> str:
        return f'{url}@{retries}'

    async with TaskPipeline() as pipeline:
        pipeline.start(functools.partial(fetch, retries=3), 'a')
        pipeline.start_many(functools.partial(fetch, retries=3), ['b', 'c'])
        pipeline.start(functools.partial(functools.partial(fetch, retries=9)), 'd')
        pipeline.start(functools.partial(Fetcher()), 'e')

    assert sorted(pipeline.results()) == [
        'Fetcher',
        'fetch',
        'fetch#1',
        'fetch#1[0]',
        'fetch#1[1]',
        'fetch#2',
    ]


async def test_pipeline_does_not_resolve_a_handle_bound_into_a_partial():
    """
    Case: bind a `TaskHandle` into a `functools.partial` instead of passing it as an argument.
    Expect: it reaches the callable unresolved — a partial is a container, so neither scan sees it.
    """
    received: list[str] = []

    async def record(_item: int, extra: object) -> None:
        received.append(type(extra).__name__)

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(double, 1, task_name='upstream')
        pipeline.start_many(functools.partial(record, extra=handle), [1], task_name='bound')

    assert received == ['TaskHandle']


async def test_depends_on_makes_result_safe_for_a_handle_bound_into_a_partial():
    """
    Case: read `result()` on a partial-bound handle, with and without declaring it in `depends_on`.
    Expect: `depends_on` orders the tasks so the read succeeds; without it the same read is too early.
    """

    async def slow() -> str:
        await asyncio.sleep(0.05)
        return 'config'

    async def read(_item: int, extra: TaskHandle[str]) -> str:
        try:
            return extra.result()
        except RuntimeError as error:
            return str(error)

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(slow, task_name='config')
        undeclared = pipeline.start_many(functools.partial(read, extra=handle), [1], task_name='undeclared')
        declared = pipeline.start_many(
            functools.partial(read, extra=handle),
            [1],
            task_name='declared',
            depends_on=[handle],
        )

    assert undeclared.result() == ["Task 'config' has not completed."]
    assert declared.result() == ['config']


async def test_a_handle_bound_into_a_partial_can_still_be_awaited():
    """
    Case: await a handle that a `functools.partial` delivered unresolved.
    Expect: it works like any other wait, with no `depends_on` needed because the wait does the ordering.
    """

    async def slow() -> str:
        await asyncio.sleep(0.05)
        return 'upstream-value'

    async def via_wait(_item: int, extra: TaskHandle[str]) -> str:
        return await extra.wait()

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(slow, task_name='upstream')
        waited = pipeline.start_many(functools.partial(via_wait, extra=handle), [1], task_name='waited')

    assert waited.result() == ['upstream-value']


async def test_pipeline_gathers_individually_started_tasks_into_one_handle():
    """
    Case: start tasks individually, then collect them with `gather`.
    Expect: one handle whose result is their results in argument order, usable as a dependency.
    """

    async def summarise(values: list[int]) -> int:
        return sum(values)

    async with TaskPipeline() as pipeline:
        shards = [pipeline.start(double, index, task_name=f'shard{index}') for index in range(4)]
        everything = pipeline.gather(*shards)
        total = pipeline.start(summarise, everything, task_name='total')

    assert everything.result() == [0, 2, 4, 6]
    assert total.result() == 12


async def test_pipeline_gather_names_itself_after_what_it_does():
    """
    Case: gather twice without naming, and once with a name.
    Expect: the barrier is named `gather`, not after the private coroutine that implements it.
    """
    async with TaskPipeline() as pipeline:
        first = pipeline.start(double, 1, task_name='first')
        second = pipeline.start(double, 2, task_name='second')

        pipeline.gather(first, second)
        pipeline.gather(first, second)
        pipeline.gather(first, task_name='pair')

    assert sorted(pipeline.results()) == ['first', 'gather', 'gather#1', 'pair', 'second']


async def test_pipeline_gather_with_no_handles_returns_an_empty_list():
    """
    Case: gather nothing.
    Expect: a handle whose result is an empty list, rather than an error.
    """
    async with TaskPipeline() as pipeline:
        nothing: TaskHandle[list[int]] = pipeline.gather()

    assert nothing.result() == []


async def test_pipeline_names_tasks_after_their_callable():
    """
    Case: start several tasks without naming them.
    Expect: names derive from the callable and stay unique with a gapless suffix.
    """
    async with TaskPipeline() as pipeline:
        for value in range(4):
            pipeline.start(double, value)

    assert sorted(pipeline.results()) == ['double', 'double#1', 'double#2', 'double#3']


async def test_pipeline_names_an_unnamed_fan_out_after_its_callable():
    """
    Case: fan a callable out without naming the barrier.
    Expect: the barrier takes the callable's name and its sub-tasks hang off it.
    """
    async with TaskPipeline() as pipeline:
        items = pipeline.start_many(double, [1, 2])

    assert items.name == 'double'
    assert sorted(pipeline.results()) == ['double', 'double[0]', 'double[1]']


async def test_pipeline_keeps_fan_out_names_unique_when_the_callable_name_is_taken():
    """
    Case: fan a callable out after an ordinary task has already claimed its name.
    Expect: the barrier moves to the next free name and its sub-tasks follow it.
    """
    async with TaskPipeline() as pipeline:
        pipeline.start(double, 0)
        items = pipeline.start_many(double, [1, 2])

    assert items.name == 'double#1'
    assert sorted(pipeline.results()) == ['double', 'double#1', 'double#1[0]', 'double#1[1]']


async def test_pipeline_rejects_a_duplicate_explicit_name():
    """
    Case: start two tasks under the same explicit name.
    Expect: `ValueError` on the second one.
    """
    async with TaskPipeline() as pipeline:
        pipeline.start(double, 1, task_name='only')

        with pytest.raises(ValueError, match='already in use'):
            pipeline.start(double, 2, task_name='only')


async def test_pipeline_start_many_starts_nothing_on_a_name_clash():
    """
    Case: `start_many` collides with an existing sub-task name.
    Expect: `ValueError` before any of its tasks are started.
    """
    async with TaskPipeline() as pipeline:
        pipeline.start(double, 0, task_name='batch[1]')

        with pytest.raises(ValueError, match='already in use'):
            pipeline.start_many(double, [1, 2, 3], task_name='batch')

    assert sorted(pipeline.results()) == ['batch[1]']


async def test_pipeline_rejects_a_handle_from_another_pipeline():
    """
    Case: pass a handle produced by a different pipeline.
    Expect: `ValueError`.
    """
    async with TaskPipeline() as other:
        foreign = other.start(double, 1)

    async with TaskPipeline() as pipeline:
        with pytest.raises(ValueError, match='not produced by this pipeline'):
            pipeline.start(double, foreign)


async def test_pipeline_respects_the_max_concurrency():
    """
    Case: start more tasks than the limit allows.
    Expect: no more than the limit execute at once.
    """
    task, peak = concurrency_tracker()

    async with TaskPipeline(max_concurrency=3) as pipeline:
        pipeline.start_many(task, range(12))

    assert peak() == 3


async def test_pipeline_starts_the_next_task_when_a_permit_frees():
    """
    Case: one permit, a short task and a long task started together.
    Expect: the long task starts as soon as the short one ends, not before.
    """
    events: list[str] = []

    async def task(spec: tuple[str, float]) -> str:
        task_id, sleep_time = spec
        events.append(f'start:{task_id}')
        await asyncio.sleep(sleep_time)
        events.append(f'end:{task_id}')

        return task_id

    async with TaskPipeline(max_concurrency=1) as pipeline:
        pipeline.start_many(task, [('short', 0.01), ('long', 0.02)])

    assert events == ['start:short', 'end:short', 'start:long', 'end:long']


async def test_pipeline_spawns_tasks_only_once_they_are_ready():
    """
    Case: build a long dependency chain.
    Expect: only the runnable link exists as an asyncio task at any moment.
    """
    peak = 0

    async def link(value: int) -> int:
        nonlocal peak
        peak = max(peak, len([task for task in asyncio.all_tasks() if task.get_name().startswith('link')]))
        return value + 1

    async with TaskPipeline(max_concurrency=8) as pipeline:
        handle = pipeline.start(double, 0, task_name='seed')

        for index in range(50):
            handle = pipeline.start(link, handle, task_name=f'link{index}')

    assert handle.result() == 50
    assert peak == 1


async def test_pipeline_awaits_inline_with_more_waiters_than_permits():
    """
    Case: more tasks wait on children than the limit allows to execute.
    Expect: every one of them completes.
    """

    async def parent(value: int) -> int:
        return await current_pipeline().start(double, value).wait()

    async with TaskPipeline(max_concurrency=2) as pipeline:
        parents = [pipeline.start(parent, value, task_name=f'parent{value}') for value in range(8)]

    assert [handle.result() for handle in parents] == [0, 2, 4, 6, 8, 10, 12, 14]


async def test_pipeline_awaits_inline_through_deep_recursion():
    """
    Case: each task starts and awaits a deeper one, with a single permit.
    Expect: the recursion unwinds instead of deadlocking.
    """

    async def descend(depth: int) -> str:
        if depth == 0:
            return 'bottom'

        return await current_pipeline().start(descend, depth - 1).wait()

    async with TaskPipeline(max_concurrency=1) as pipeline:
        handle = pipeline.start(descend, 5)

    assert handle.result() == 'bottom'


async def test_pipeline_rejects_a_task_waiting_on_itself():
    """
    Case: a task awaits its own handle.
    Expect: `RuntimeError` at the wait, rather than a pipeline that never finishes.
    """
    handles: dict[str, TaskHandle[str]] = {}

    async def task() -> str:
        return await handles['me'].wait()

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            handles['me'] = pipeline.start(task, task_name='me')

    error = single_error(exc_info, RuntimeError)
    assert str(error) == (
        "Task 'me' cannot wait on task 'me': that would deadlock, because 'me' is itself blocked on 'me'."
    )


async def test_pipeline_rejects_two_tasks_waiting_on_each_other():
    """
    Case: two tasks await each other's handles.
    Expect: the wait that closes the cycle raises instead of hanging.
    """
    handles: dict[str, TaskHandle[str]] = {}

    async def task_a() -> str:
        return await handles['b'].wait()

    async def task_b() -> str:
        return await handles['a'].wait()

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            handles['a'] = pipeline.start(task_a, task_name='a')
            handles['b'] = pipeline.start(task_b, task_name='b')

    assert 'that would deadlock' in str(single_error(exc_info, RuntimeError))


async def test_pipeline_rejects_a_cycle_closed_through_a_dependency():
    """
    Case: a task awaits another that cannot start until the waiter finishes.
    Expect: the cycle is caught even though only its `depends_on` half is in the graph.
    """
    handles: dict[str, TaskHandle[str]] = {}

    async def parent() -> str:
        return await handles['later'].wait()

    async def later() -> str:
        return 'later'

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            upstream = pipeline.start(parent, task_name='parent')
            handles['later'] = pipeline.start(later, task_name='later', depends_on=[upstream])

    error = single_error(exc_info, RuntimeError)
    assert str(error) == (
        "Task 'parent' cannot wait on task 'later': that would deadlock, because 'later' is itself blocked on 'parent'."
    )


async def test_pipeline_allows_several_tasks_to_wait_on_one_handle():
    """
    Case: two tasks wait on the same handle.
    Expect: the shared target is not mistaken for a cycle. Chains of waits are covered by the
    deep-recursion test.
    """

    async def leaf(value: int) -> int:
        await asyncio.sleep(0.01)
        return value

    async with TaskPipeline(max_concurrency=2) as pipeline:
        shared = pipeline.start(leaf, 99, task_name='shared')

        async def consume() -> int:
            return await shared.wait()

        both = [pipeline.start(consume, task_name=f'consumer{index}') for index in range(2)]

    assert [handle.result() for handle in both] == [99, 99]


async def test_handle_reports_whether_the_task_is_done():
    """
    Case: read `done()` before the task has run, and after the scope closes.
    Expect: `False` while it is outstanding, `True` once it has settled either way.
    """
    async with TaskPipeline() as pipeline:
        handle = pipeline.start(double, 1, task_name='task')

        assert handle.done() is False

    assert handle.done() is True


async def test_pipeline_still_bounds_execution_while_tasks_wait():
    """
    Case: many tasks await children at once.
    Expect: waiting tasks do not count towards the limit, and executing ones still respect it.
    """
    work, peak = concurrency_tracker()

    async def waiter(value: int) -> int:
        return await current_pipeline().start(work, value).wait()

    async with TaskPipeline(max_concurrency=3) as pipeline:
        pipeline.start_many(waiter, range(10))

    assert peak() == 3


async def test_pipeline_start_many_works_from_inside_a_task():
    """
    Case: a running task fans work out through `current_pipeline().start_many`.
    Expect: it reaches the ambient pipeline and the results come back in order.
    """

    async def parent() -> list[int]:
        return await current_pipeline().start_many(double, [1, 2, 3]).wait()

    async with TaskPipeline(max_concurrency=2) as pipeline:
        handle = pipeline.start(parent)

    assert handle.result() == [2, 4, 6]


async def test_current_pipeline_resolves_inside_a_task_and_raises_outside():
    """
    Case: ask for the ambient pipeline from inside a task, then from outside one.
    Expect: the running pipeline inside, and `RuntimeError` outside.
    """

    async def task() -> TaskPipeline:
        return current_pipeline()

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(task)

    assert handle.result() is pipeline

    with pytest.raises(RuntimeError, match='No pipeline is open'):
        current_pipeline()


async def test_handle_awaited_after_the_pipeline_closed_returns_its_result():
    """
    Case: await a handle whose task already finished, outside any pipeline task.
    Expect: the result, since there is nothing to wait for.
    """
    async with TaskPipeline() as pipeline:
        handle = pipeline.start(double, 4)

    assert await handle.wait() == 8


async def test_handle_awaited_without_a_permit_is_rejected():
    """
    Case: await an unfinished handle from a task that owns no permit.
    Expect: `RuntimeError`, rather than a silently inflated concurrency limit.
    """
    captured: list[str] = []

    async def slow() -> str:
        await asyncio.sleep(0.05)
        return 'done'

    async def outsider(handle: TaskHandle[str]) -> None:
        try:
            await handle.wait()
        except RuntimeError as error:
            captured.append(str(error))

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(slow)
        await asyncio.create_task(outsider(handle))

    assert 'can only be waited on from inside a running pipeline task' in captured[0]


async def test_a_task_spawned_by_a_task_cannot_hand_back_its_parents_permit():
    """
    Case: a raw asyncio task started inside a pipeline task inherits its context and awaits a handle.
    Expect: it is rejected on task identity, while the owning task keeps working.
    """
    captured: list[str] = []

    async def slow() -> int:
        await asyncio.sleep(0.05)
        return 2

    async def parent() -> int:
        handle = current_pipeline().start(slow)

        async def intruder() -> None:
            try:
                await handle.wait()
            except RuntimeError as error:
                captured.append(str(error))

        await asyncio.create_task(intruder())

        return await handle.wait()

    async with TaskPipeline(max_concurrency=2) as pipeline:
        handle = pipeline.start(parent)

    assert 'can only be waited on from inside a running pipeline task' in captured[0]
    assert handle.result() == 2


async def test_pipeline_ends_on_a_failure_nobody_is_awaiting():
    """
    Case: a task fails with no one awaiting it.
    Expect: the scope raises an `ExceptionGroup` carrying the original error and its task name.
    """
    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            failing = pipeline.start(boom, task_name='failing')

    error = single_error(exc_info, ValueError)
    assert error.__notes__ == ["Raised by pipeline task 'failing'."]

    with pytest.raises(ValueError, match='boom'):
        _ = failing.result()


async def test_pipeline_contains_a_failure_its_owner_handles():
    """
    Case: a task awaits a child that fails, and handles the error.
    Expect: the pipeline carries on and unrelated tasks still finish.
    """

    async def parent() -> str:
        try:
            await current_pipeline().start(boom).wait()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    async with TaskPipeline() as pipeline:
        handled = pipeline.start(parent)
        sibling = pipeline.start(double, 21, task_name='sibling')

    assert handled.result() == 'handled: boom'
    assert sibling.result() == 42


async def test_pipeline_contains_a_failure_that_lands_before_the_owner_waits():
    """
    Case: a task starts a child, works for longer than the child takes to fail, then waits on it.
    Expect: the failure is held for the owner and delivered to its `try`/`except`, regardless of timing.
    """

    async def parent() -> str:
        child = current_pipeline().start(boom, task_name='child')
        await asyncio.sleep(0.05)
        assert child.done()

        try:
            await child.wait()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    async with TaskPipeline() as pipeline:
        handled = pipeline.start(parent, task_name='parent')

    assert handled.result() == 'handled: boom'


async def test_pipeline_contains_a_failure_a_sibling_handles_while_the_owner_runs():
    """
    Case: a task starts a child and a sibling that awaits the child through a bound handle.
    Expect: the owner holds the failure, the sibling reads it, and the owner finishes clean.
    """

    async def sibling(child: TaskHandle[None]) -> str:
        try:
            await child.wait()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    async def parent() -> str:
        pipeline = current_pipeline()
        child = pipeline.start(boom, task_name='child')
        return await pipeline.start(functools.partial(sibling, child), task_name='sibling').wait()

    async with TaskPipeline() as pipeline:
        handled = pipeline.start(parent, task_name='parent')

    assert handled.result() == 'handled: boom'


async def test_pipeline_ends_when_the_owner_finishes_without_reading_a_failure():
    """
    Case: a task starts a child that fails, never reads the failure and returns normally.
    Expect: the owner fails with the child's error, the error says so, and the pipeline ends.
    """

    async def parent() -> str:
        current_pipeline().start(boom, task_name='child')
        await asyncio.sleep(0.02)
        return 'returned'

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            parent_handle = pipeline.start(parent, task_name='parent')

    error = single_error(exc_info, ValueError)
    assert error.__notes__ == [
        "Raised by pipeline task 'child'.",
        "Not handled by its owner, pipeline task 'parent'.",
    ]

    with pytest.raises(ValueError, match='boom'):
        _ = parent_handle.result()


@pytest.mark.parametrize('child_delay', [0.0, 0.05], ids=['before owner returns', 'after owner returns'])
async def test_pipeline_lets_a_grandparent_handle_a_failure_on_either_side_of_the_owner_returning(
    child_delay: float,
):
    """
    Case: a grandparent starts a parent, which starts a child, returns its handle without reading it, and
        the child fails either before or after the parent has returned.
    Expect: the grandparent handles the failure either way — from the parent when it was held there, and
        from the child directly when it passed up the chain on the parent's return.
    """

    async def slow_boom() -> None:
        await asyncio.sleep(child_delay)
        raise ValueError('boom')

    async def parent() -> TaskHandle[None]:
        child = current_pipeline().start(slow_boom, task_name='child')
        await asyncio.sleep(0.02)
        return child

    async def grandparent() -> str:
        try:
            child = await current_pipeline().start(parent, task_name='parent').wait()
            await child.wait()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(grandparent, task_name='grandparent')

    assert handle.result() == 'handled: boom'


async def test_pipeline_names_the_original_owner_of_a_failure_that_passed_up_the_chain():
    """
    Case: a parent starts a slow child and returns at once; the child then fails and the grandparent
        returns without reading it.
    Expect: the grandparent fails with the child's error, and the notes say who started the child.
    """

    async def slow_boom() -> None:
        await asyncio.sleep(0.02)
        raise ValueError('boom')

    async def parent() -> None:
        current_pipeline().start(slow_boom, task_name='child')

    async def grandparent() -> str:
        await current_pipeline().start(parent, task_name='parent').wait()
        await asyncio.sleep(0.05)
        return 'returned'

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            handle = pipeline.start(grandparent, task_name='grandparent')

    error = single_error(exc_info, ValueError)
    assert error.__notes__ == [
        "Raised by pipeline task 'child'.",
        "Task 'child' was started by 'parent', which had already finished.",
        "Not handled by its owner, pipeline task 'grandparent'.",
    ]

    with pytest.raises(ValueError, match='boom'):
        _ = handle.result()


async def test_pipeline_ends_when_a_passed_up_failure_has_nobody_left_to_hold_it():
    """
    Case: a parent starts a slow child, wires a successor to it and returns; the grandparent returns too;
        only then does the child fail, with every ancestor gone.
    Expect: the child passed up to the body, which cannot hold a failure, so the pipeline ends, the
        successor is discarded, and the error still names the task that started the child.
    """

    async def slow_boom() -> None:
        await asyncio.sleep(0.03)
        raise ValueError('boom')

    async def parent() -> TaskHandle[int]:
        pipeline = current_pipeline()
        child = pipeline.start(slow_boom, task_name='child')
        return pipeline.start(double, child, task_name='successor')

    async def grandparent() -> TaskHandle[int]:
        return await current_pipeline().start(parent, task_name='parent').wait()

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            grandparent_handle = pipeline.start(grandparent, task_name='grandparent')

    error = single_error(exc_info, ValueError)
    assert error.__notes__ == [
        "Raised by pipeline task 'child'.",
        "Task 'child' was started by 'parent', which had already finished.",
    ]

    with pytest.raises(RuntimeError, match='was discarded'):
        _ = grandparent_handle.result().result()


async def test_pipeline_passes_a_failing_owners_running_children_up_the_chain():
    """
    Case: a parent starts a child that will succeed, then fails itself; the grandparent handles the
        parent's error and waits on the child.
    Expect: the child keeps running under the grandparent and delivers its result.
    """
    handles: dict[str, TaskHandle[int]] = {}

    async def child() -> int:
        await asyncio.sleep(0.02)
        return 7

    async def parent() -> None:
        handles['child'] = current_pipeline().start(child, task_name='child')
        raise KeyError('parent')

    async def grandparent() -> int:
        try:
            await current_pipeline().start(parent, task_name='parent').wait()
        except KeyError:
            pass

        return await handles['child'].wait()

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(grandparent, task_name='grandparent')

    assert handle.result() == 7


async def test_pipeline_ends_on_a_failure_a_non_owner_awaits():
    """
    Case: the body starts a failing task and a sibling that awaits it with `try`/`except`.
    Expect: the body cannot hold a failure, so the pipeline ends even though the sibling would handle it.
    """

    async def sibling(child: TaskHandle[None]) -> str:
        try:
            await child.wait()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            child = pipeline.start(boom, task_name='child')
            pipeline.start(functools.partial(sibling, child), task_name='sibling')

    single_error(exc_info, ValueError)


async def test_pipeline_notes_child_failures_a_failing_owner_left_unhandled():
    """
    Case: a task starts two children that fail, reads neither, and then fails itself.
    Expect: the owner's own error surfaces, annotated with the children's errors so they are not lost.
    """

    async def parent() -> None:
        pipeline = current_pipeline()
        pipeline.start(boom, task_name='first')
        pipeline.start(boom, task_name='second')
        await asyncio.sleep(0.02)
        raise KeyError('parent')

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            pipeline.start(parent, task_name='parent')

    error = single_error(exc_info, KeyError)
    assert error.__notes__ == [
        "Raised by pipeline task 'parent'.",
        "Task 'parent' also left unhandled: ValueError('boom').",
        "Task 'parent' also left unhandled: ValueError('boom').",
    ]


async def test_pipeline_counts_reading_a_failure_with_result_as_handling_it():
    """
    Case: a task starts a child, lets it fail, and reads the error with `result()` rather than `wait()`.
    Expect: that counts as handling it, so the owner finishes clean.
    """

    async def parent() -> str:
        child = current_pipeline().start(boom, task_name='child')
        await asyncio.sleep(0.02)

        try:
            child.result()
        except ValueError as error:
            return f'handled: {error}'

        return 'not reached'

    async with TaskPipeline() as pipeline:
        handled = pipeline.start(parent, task_name='parent')

    assert handled.result() == 'handled: boom'


async def test_pipeline_discards_everything_downstream_of_a_failure():
    """
    Case: a failing task has dependents, and those dependents have dependents.
    Expect: the whole downstream cone is discarded and says so.
    """

    async def join(left: int, right: int) -> tuple[int, int]:
        return left, right

    with pytest.raises(ExceptionGroup):
        async with TaskPipeline() as pipeline:
            root = pipeline.start(boom, task_name='root')
            left = pipeline.start(double, root, task_name='left')
            right = pipeline.start(double, root, task_name='right')
            tip = pipeline.start(join, left, right, task_name='tip')

    for handle in (left, right, tip):
        with pytest.raises(RuntimeError, match='was discarded'):
            _ = handle.result()


async def test_pipeline_discards_a_node_it_can_no_longer_spawn() -> None:
    """
    Case: a task swallows its cancellation and completes while the pipeline is unwinding, releasing a
        dependent that the task group will no longer accept.
    Expect: the dependent is discarded rather than left pending, and only the real failure propagates.
    """

    async def cleans_up_on_cancel() -> str:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            return 'cleaned-up'

        return 'never-cancelled'

    async def consume(value: str) -> str:
        return value

    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            slow = pipeline.start(cleans_up_on_cancel, task_name='slow')
            dependent = pipeline.start(consume, slow, task_name='dependent')
            pipeline.start(boom, task_name='failing')

    # Without the guard the failed spawn joins the group as a second, unrelated error.
    assert [type(error) for error in exc_info.value.exceptions] == [ValueError]

    # The cleanup task really did finish, which is what releases the dependent mid-shutdown.
    assert slow.result() == 'cleaned-up'

    # And the dependent is settled as discarded, not left pending as "has not completed".
    with pytest.raises(RuntimeError, match='was discarded'):
        _ = dependent.result()


def not_async(value: int = 1) -> int:
    """
    Return a plain value, so that starting it as a task is an error.
    """
    return value


@pytest.mark.parametrize('fn', [not_async, functools.partial(not_async, 2)], ids=['plain', 'partial'])
async def test_pipeline_reports_a_callable_that_is_not_async(fn: Callable[..., Any]):
    """
    Case: start a callable that returns a plain value, directly and through a `functools.partial`.
    Expect: a `TypeError` naming the function underneath and what it returned.
    """
    with pytest.raises(ExceptionGroup) as exc_info:
        async with TaskPipeline() as pipeline:
            pipeline.start(fn)

    error = single_error(exc_info, TypeError)
    assert str(error).endswith('not_async() returned int, not an awaitable; pipeline tasks must be async callables.')


async def test_pipeline_accepts_any_callable_returning_an_awaitable():
    """
    Case: start callables that are not coroutine functions but do return awaitables.
    Expect: all of them run.
    """

    class Fetcher:
        async def __call__(self, value: str) -> str:
            return value

    async with TaskPipeline() as pipeline:
        callable_object = pipeline.start(Fetcher(), 'a', task_name='object')
        lambda_task = pipeline.start(lambda: double(2), task_name='lambda')

    assert callable_object.result() == 'a'
    assert lambda_task.result() == 4


async def test_pipeline_results_track_the_run():
    """
    Case: read `results()` before, during and after the pipeline runs.
    Expect: empty, then partial, then complete.
    """
    pipeline = TaskPipeline()

    assert pipeline.results() == {}

    async def peek() -> dict[str, Any]:
        return dict(pipeline.results())

    async with pipeline:
        first = pipeline.start(double, 1, task_name='first')
        pipeline.start(peek, depends_on=[first], task_name='peek')

    assert pipeline.results()['peek'] == {'first': 2}
    assert pipeline.results() == {'first': 2, 'peek': {'first': 2}}


async def test_pipeline_rejects_starting_outside_its_scope():
    """
    Case: call `start()` before opening the pipeline and after closing it.
    Expect: `RuntimeError` both times.
    """
    pipeline = TaskPipeline()

    with pytest.raises(RuntimeError, match='not open'):
        pipeline.start(double, 1)

    async with pipeline:
        pass

    with pytest.raises(RuntimeError, match='not open'):
        pipeline.start(double, 1)


async def test_pipeline_cannot_be_reopened():
    """
    Case: enter the same pipeline a second time.
    Expect: `RuntimeError`.
    """
    pipeline = TaskPipeline()

    async with pipeline:
        pass

    with pytest.raises(RuntimeError, match='only be opened once'):
        async with pipeline:
            pass


@pytest.mark.parametrize('max_concurrency', [0, -1])
def test_pipeline_rejects_a_non_positive_max_concurrency(max_concurrency):
    """
    Case: construct a pipeline with a limit below one.
    Expect: `ValueError` at construction rather than a hang.
    """
    with pytest.raises(ValueError, match='max_concurrency must be at least 1'):
        TaskPipeline(max_concurrency=max_concurrency)


async def test_pipeline_unwinds_under_an_outer_timeout():
    """
    Case: wrap the pipeline in `asyncio.timeout` and let a task overrun it.
    Expect: `TimeoutError` rather than a hang, and the task reports as discarded.
    """

    async def slow() -> None:
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            async with TaskPipeline() as pipeline:
                handle = pipeline.start(slow)

    with pytest.raises(RuntimeError, match='was discarded'):
        _ = handle.result()


async def test_handle_is_generic_in_its_result_type():
    """
    Case: annotate handles with their result type and run the pipeline.
    Expect: subscription works at runtime and dependency wiring is unaffected.
    """
    async with TaskPipeline() as pipeline:
        produced: TaskHandle[int] = pipeline.start(double, 21)
        doubled: TaskHandle[int] = pipeline.start(double, produced)

    assert isinstance(produced, TaskHandle)
    assert doubled.result() == 84


async def test_pipeline_runs_every_task_in_its_callers_context():
    """
    Case: dependencies set a `ContextVar` and finish in different orders, and a task starts a child.
    Expect: each task runs in the context of whoever started it, not of whichever dependency finished last.
    """
    marker: contextvars.ContextVar[str] = contextvars.ContextVar('marker', default='unset')

    async def dependency(label: str, delay: float) -> str:
        marker.set(label)
        await asyncio.sleep(delay)

        return label

    async def read_marker(*_values: str) -> str:
        return marker.get()

    async def parent() -> str:
        marker.set('parent')

        return await current_pipeline().start(read_marker, task_name='child').wait()

    async def run(first_delay: float, second_delay: float) -> str:
        marker.set('body')

        async with TaskPipeline() as pipeline:
            first = pipeline.start(dependency, 'first', first_delay, task_name='first')
            second = pipeline.start(dependency, 'second', second_delay, task_name='second')
            dependent = pipeline.start(read_marker, first, second, task_name='dependent')

        return dependent.result()

    # Whichever dependency finishes last, the dependent still sees the context `start()` was called in.
    assert await run(0.02, 0.01) == 'body'
    assert await run(0.01, 0.02) == 'body'

    marker.set('body')

    async with TaskPipeline() as pipeline:
        nested = pipeline.start(parent, task_name='parent')

    # A task that starts a child passes its own context down, not the body's.
    assert nested.result() == 'parent'


async def test_pipeline_discards_a_task_started_on_an_already_failed_dependency():
    """
    Case: a task contains a child's failure, then starts new work depending on that failed handle.
    Expect: the new task is discarded, the contained failure is not raised a second time, and the
        original exception keeps its single note.
    """

    async def consume(value: str) -> str:
        return value

    async def parent() -> str:
        pipeline = current_pipeline()
        child = pipeline.start(boom, task_name='child')

        try:
            await child.wait()
        except ValueError as error:
            notes = len(error.__notes__)

        late = pipeline.start(consume, child, task_name='late')

        with pytest.raises(RuntimeError, match='was discarded'):
            late.result()

        return f'notes={notes}'

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(parent, task_name='parent')

    assert handle.result() == 'notes=1'


async def test_pipeline_discards_a_task_whose_dependency_was_itself_discarded():
    """
    Case: start work depending on a handle that was discarded when its own dependency failed.
    Expect: the new task is discarded too, rather than failing with the discard error and ending the run.
    """

    async def consume(value: str) -> str:
        return value

    async def parent() -> str:
        pipeline = current_pipeline()
        child = pipeline.start(boom, task_name='child')
        discarded = pipeline.start(consume, child, task_name='discarded')

        try:
            await child.wait()
        except ValueError:
            pass

        await asyncio.sleep(0)
        late = pipeline.start(consume, discarded, task_name='late')

        with pytest.raises(RuntimeError, match='was discarded'):
            late.result()

        return 'contained'

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(parent, task_name='parent')

    assert handle.result() == 'contained'


async def test_pipeline_does_not_revive_a_discarded_task_when_its_other_dependency_lands():
    """
    Case: start a task depending on both a failed handle and one that is still running.
    Expect: it stays discarded — the pending dependency completing must not spawn it.
    """

    async def consume(*_values: object) -> str:
        return 'ran'

    async def slow() -> str:
        await asyncio.sleep(0.02)

        return 'slow'

    async def parent() -> str:
        pipeline = current_pipeline()
        child = pipeline.start(boom, task_name='child')
        pending = pipeline.start(slow, task_name='slow')

        try:
            await child.wait()
        except ValueError:
            pass

        mixed = pipeline.start(consume, child, pending, task_name='mixed')
        await pending.wait()

        with pytest.raises(RuntimeError, match='was discarded'):
            mixed.result()

        return 'stayed discarded'

    async with TaskPipeline() as pipeline:
        handle = pipeline.start(parent, task_name='parent')

    assert handle.result() == 'stayed discarded'


async def test_pipeline_releases_task_inputs_once_they_are_consumed():
    """
    Case: fan out over large inputs, then drop every other reference to them.
    Expect: the pipeline does not keep them alive, whether the tasks ran or were discarded.
    """

    class Payload:
        __slots__ = ('__weakref__',)

    async def consume(_payload: object) -> str:
        return 'consumed'

    async def parent() -> str:
        pipeline = current_pipeline()
        child = pipeline.start(boom, task_name='child')
        discarded_inputs = [Payload() for _ in range(3)]
        discarded_refs.extend(weakref.ref(payload) for payload in discarded_inputs)
        pipeline.start_many(consume, discarded_inputs, task_name='never', depends_on=[child])

        del discarded_inputs

        try:
            await child.wait()
        except ValueError:
            pass

        await asyncio.sleep(0)

        return 'contained'

    discarded_refs: list[weakref.ref[object]] = []

    async with TaskPipeline() as pipeline:
        executed_inputs = [Payload() for _ in range(3)]
        executed_refs = [weakref.ref(payload) for payload in executed_inputs]
        pipeline.start_many(consume, executed_inputs, task_name='executed')
        pipeline.start(parent, task_name='parent')

        del executed_inputs

    gc.collect()

    assert [ref() for ref in executed_refs] == [None, None, None]
    assert [ref() for ref in discarded_refs] == [None, None, None]

    # The captured contexts go too: each one holds every `ContextVar` value its starter had.
    assert [node.context for node in pipeline._nodes.values()] == [None] * len(pipeline._nodes)


async def test_pipeline_keeps_the_limit_after_a_waiter_handles_a_failure():
    """
    Case: several tasks await a child that fails, catch it, then do more work.
    Expect: the permit is reclaimed before the `except` block runs, so the limit still holds there.
    """
    work, peak = concurrency_tracker()

    async def failing() -> int:
        raise ValueError('child')

    async def handler(value: int) -> int:
        pipeline = current_pipeline()

        try:
            await pipeline.start(failing, task_name=f'failing{value}').wait()
        except ValueError:
            return await work(value)

        return -1

    async with TaskPipeline(max_concurrency=2) as pipeline:
        handlers = [pipeline.start(handler, value, task_name=f'handler{value}') for value in range(8)]

    assert [handle.result() for handle in handlers] == list(range(8))
    assert peak() == 2


async def test_pipeline_keeps_the_limit_when_a_wait_times_out():
    """
    Case: a task times out waiting on a child, catches the `TimeoutError` and carries on.
    Expect: it reclaims its permit before continuing — `timeout()` cancels the wait but the task lives
        on, so skipping the re-acquire would let it run outside the limit.
    """
    live = 0
    peak = 0

    async def tracked(seconds: float) -> str:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(seconds)
        live -= 1

        return 'done'

    async def child() -> str:
        return await tracked(0.08)

    async def parent() -> str:
        handle = current_pipeline().start(child, task_name='child')

        try:
            async with asyncio.timeout(0.02):
                await handle.wait()
        except TimeoutError:
            pass

        return await tracked(0.05)

    async with TaskPipeline(max_concurrency=1) as pipeline:
        handle = pipeline.start(parent, task_name='parent')

    assert handle.result() == 'done'
    assert peak == 1
