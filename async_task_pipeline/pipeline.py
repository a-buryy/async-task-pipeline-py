"""
Run async tasks with dependencies inside a `TaskGroup`-style scope.

A pipeline is a scope: open it with `async with`, start tasks inside it, and leaving the scope waits for
them. Tasks are plain async callables. Pass a `TaskHandle` as a direct argument to wire a dependency, and
the pipeline replaces the handle with its task's result before invoking the callable; handles nested in
containers are not inspected. Or `await handle.wait()` to consume a result inline, which is safe at any
`concurrency_limit` because a waiting task gives its permit back first.

`start()` schedules immediately and behaves the same whether it is called from the `async with` body or
from inside a running task. A started task becomes an asyncio task only once its dependencies are
satisfied, so tasks still waiting on dependencies hold no `asyncio.Task`.

A failure is held for the task that started it, which can handle it through `wait()` or `result()`. A
failure with no running owner to hold it ends the pipeline and surfaces as an `ExceptionGroup`.
"""

import asyncio
import contextvars
import functools
import inspect
import logging
import time
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from dataclasses import (
    dataclass,
    field,
)
from types import TracebackType
from typing import (
    Any,
    Self,
)


logger = logging.getLogger(__name__)

_current_pipeline: contextvars.ContextVar['TaskPipeline | None'] = contextvars.ContextVar(
    'current_pipeline',
    default=None,
)
_current_permit: contextvars.ContextVar['_Permit | None'] = contextvars.ContextVar(
    'current_permit',
    default=None,
)


class _Permit:
    """
    A task's claim on the concurrency limiter, which it hands back while waiting on another task.
    """

    def __init__(
        self,
        semaphore: asyncio.Semaphore,
        task: 'asyncio.Task[None] | None',
        handle: 'TaskHandle[Any]',
    ):
        """
        Construct the object.

        Args:
            semaphore: shared concurrency limiter to claim from.
            task: asyncio task this permit belongs to, used to reject claims from any other task.
            handle: handle of the task holding this permit, so a wait can tell who is waiting.
        """
        self._semaphore = semaphore
        self.task = task
        self.handle = handle
        self.held = False

    async def acquire(self) -> None:
        """
        Claim the permit, unless it is already held.
        """
        if not self.held:
            await self._semaphore.acquire()
            self.held = True

    def release(self) -> None:
        """
        Hand the permit back, unless it is already handed back.
        """
        if self.held:
            self.held = False
            self._semaphore.release()


def _owned_permit() -> _Permit | None:
    """
    Get the permit held by the current asyncio task, or `None` when it holds none.

    A task spawned inside a pipeline task inherits that task's context, and with it the permit, so the
    owner is checked by identity rather than trusting the context alone.
    """
    permit = _current_permit.get()

    if permit is None or permit.task is not asyncio.current_task():
        return None

    return permit


class TaskHandle[T]:
    """
    Handle for one task in a pipeline, generic in the type its callable returns.

    Pass it as an argument to another task to wire a dependency, `await wait()` to consume its result
    from inside a running task, or call `result()` once the pipeline has closed.
    """

    def __init__(self, name: str, *, owner: 'TaskHandle[Any] | None' = None):
        """
        Construct the object.

        Args:
            name: unique task name, used for diagnostics and as the key in `TaskPipeline.results()`.
            owner: handle of the task that started this one, or `None` when it was started from the
                scope body. The owner is answerable for a failure nobody else handles, and this handle
                registers itself with it.
        """
        self.name = name
        self._future: asyncio.Future[T] | None = None
        self._owner = owner
        self._started_by = None if owner is None else owner.name
        self._owned: list[TaskHandle[Any]] = []
        self._failure_retrieved = False
        self._blocked_on: tuple[TaskHandle[Any], ...] = ()

        if owner is not None:
            owner._owned.append(self)

    def result(self) -> T:
        """
        Get the task's result without waiting.

        Raises:
            RuntimeError: if the task has not completed, or was discarded because a dependency did
                not produce a result.
            Exception: whatever the task's callable raised, if it failed.

        Returns:
            The value the callable returned.
        """
        future = self._future

        if future is None or not future.done():
            raise RuntimeError(f"Task '{self.name}' has not completed.")

        if future.cancelled():
            raise RuntimeError(
                f"Task '{self.name}' was discarded, most likely because a dependency did not complete.",
            )

        if future.exception() is not None:
            self._failure_retrieved = True

        return future.result()

    def done(self) -> bool:
        """
        Tell whether the task has finished, one way or another.

        Returns:
            `True` once the task has produced a result, failed, been cancelled or been discarded.
        """
        return self._future is not None and self._future.done()

    async def wait(self) -> T:
        """
        Wait for the task from inside another running task, then return its result.

        The waiting task hands its concurrency permit back for the duration and claims one again before
        returning, so waiting inline is safe at any concurrency limit.

        Raises:
            RuntimeError: if called outside the task that owns a permit, if this task was discarded, or
                if waiting would close a cycle and deadlock.
            Exception: whatever the task's callable raised, if it failed.

        Returns:
            The value the callable returned.
        """
        future = self._future

        if future is None:
            raise RuntimeError(f"Task '{self.name}' does not belong to a running pipeline.")

        if not future.done():
            permit = _owned_permit()

            if permit is None:
                raise RuntimeError(
                    f"Task '{self.name}' can only be waited on from inside a running pipeline task; "
                    f'start the work that needs it as a task, or read `result()` once the pipeline has closed.',
                )

            pipeline = _current_pipeline.get()

            if pipeline is not None:
                pipeline._guard_no_wait_cycle(permit.handle, self)

            permit.handle._blocked_on = (self,)
            permit.release()

            try:
                logger.debug('Waiting on task %r; concurrency permit released.', self.name)
                # `asyncio.wait` so cancelling a waiter never cancels the awaited task's future.
                await asyncio.wait([future])
            finally:
                permit.handle._blocked_on = ()
                await permit.acquire()

        return self.result()

    def _hand_over_owned(self) -> list[tuple['TaskHandle[Any]', BaseException]]:
        """
        Give up ownership as this task finishes.

        Tasks it owns that are still running pass to its own owner, so a later failure is held by the
        nearest running ancestor rather than falling out of the chain. Tasks that already failed without
        anyone reading the failure are returned, for this task to answer for.

        Returns:
            Pairs of failed handle and its exception, in the order the tasks were started.
        """
        owned, self._owned = self._owned, []
        unhandled: list[tuple[TaskHandle[Any], BaseException]] = []

        for child in owned:
            future = child._future

            if future is None:
                continue

            if not future.done():
                child._owner = self._owner

                if self._owner is not None:
                    self._owner._owned.append(child)

                logger.debug('Task %r passed from %r to its owner %r.', child.name, self.name, self._owner)
                continue

            if child._failure_retrieved or future.cancelled():
                continue

            error = future.exception()

            if error is not None:
                unhandled.append((child, error))

        return unhandled

    def __repr__(self) -> str:
        """
        Return a debug-friendly representation.
        """
        return f'TaskHandle({self.name!r})'


@dataclass(slots=True)
class _Node:
    """
    Internal record describing one task and its place in the dependency graph.
    """

    handle: TaskHandle[Any]
    fn: Callable[..., Awaitable[Any]]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    context: contextvars.Context | None = None
    pending_dependencies: int = 0
    dependents: list['_Node'] = field(default_factory=list)

    def release_arguments(self) -> None:
        """
        Drop the call arguments once the task has been given them, or will never run.
        """
        self.args = ()
        self.kwargs = {}


def _unwrap_callable(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """
    Look through any `functools.partial` layers to the callable underneath.

    Args:
        fn: callable to unwrap.

    Returns:
        The innermost callable, or `fn` itself if it is not a partial.
    """
    while isinstance(fn, functools.partial):
        fn = fn.func
    return fn


async def _collect_list(*results: Any) -> list[Any]:
    """
    Collect results into a list. Used as the barrier task for `start_many`.
    """
    return list(results)


class TaskPipeline:
    """
    Scoped pipeline of async tasks with a bounded number of them executing at once.

    Open it with `async with`, start tasks inside, and leave the scope to wait for everything to finish::

        async with TaskPipeline(concurrency_limit=5) as pipeline:
            users = pipeline.start(fetch_users)
            orders = pipeline.start(fetch_orders)
            report = pipeline.start(build_report, users, orders)

        print(report.result())

    A running task starts further tasks the same way, reaching its pipeline with `current_pipeline()`, and
    consumes them with `await handle.wait()`. `concurrency_limit` bounds tasks *executing*: a task that is
    waiting on another one hands its permit back, so waiting inline never starves the task waited for.

    Failures have an owner. A task started from inside a running task is owned by that task: while the
    owner runs, the exception is delivered to whoever waits on the failed task and the pipeline carries on,
    so `try`/`except` around `await handle.wait()` is meaningful. If the owner finishes without anyone
    having read the failure, the owner fails with it; tasks still running when their owner finishes pass
    to the owner's own owner. A task with no owner — started from the `async with` body, or every ancestor
    finished — ends the pipeline at once when it fails, surfacing as an `ExceptionGroup`. Tasks that
    depended on a failed task are discarded.
    """

    _min_concurrency_limit = 1

    def __init__(self, concurrency_limit: int = 10):
        """
        Construct the object.

        Args:
            concurrency_limit: maximum number of tasks executing at once. Tasks awaiting another task do
                not count towards it.

        Raises:
            ValueError: if `concurrency_limit` is less than 1.
        """
        if concurrency_limit < self._min_concurrency_limit:
            raise ValueError(
                f'concurrency_limit must be at least {self._min_concurrency_limit}, got {concurrency_limit}.',
            )

        self.concurrency_limit = concurrency_limit
        self._nodes: dict[TaskHandle[Any], _Node] = {}
        self._names: set[str] = set()
        self._name_counters: dict[str, int] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._task_group: asyncio.TaskGroup | None = None
        self._pipeline_token: contextvars.Token['TaskPipeline | None'] | None = None
        self._closed = False

    def results(self) -> dict[str, Any]:
        """
        Build a mapping of every task that completed successfully, keyed by task name.

        Walks the graph and returns a fresh snapshot on every call, so keep the mapping rather than
        calling this repeatedly. Callable at any point: empty before the pipeline opens, partial while it
        runs, and after a failure it holds whatever the pipeline did produce. Failed and discarded tasks
        are omitted — read their `TaskHandle.result()` to see why.

        Returns:
            Mapping of task name to result value.
        """
        return {handle.name: handle.result() for handle in self._nodes if self._has_result(handle)}

    async def __aenter__(self) -> Self:
        """
        Open the pipeline.

        Raises:
            RuntimeError: if this pipeline has already been opened.

        Returns:
            This pipeline.
        """
        if self._task_group is not None or self._closed:
            raise RuntimeError('A pipeline can only be opened once; create a new one for another run.')

        logger.debug('Pipeline opened with concurrency limit %d.', self.concurrency_limit)

        self._loop = asyncio.get_running_loop()
        self._semaphore = asyncio.Semaphore(self.concurrency_limit)
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._pipeline_token = _current_pipeline.set(self)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Wait for every started task, then close the pipeline.

        Args:
            exc_type: type of the exception raised in the `async with` body, if any.
            exc: exception raised in the `async with` body, if any.
            traceback: traceback of the exception raised in the `async with` body, if any.

        Raises:
            ExceptionGroup: if any task failed with no running owner to hold the failure.
        """
        task_group = self._task_group

        try:
            # Kept open across the drain: tasks still finishing release their dependents, and those
            # dependents have to be spawnable.
            if task_group is not None:
                await task_group.__aexit__(exc_type, exc, traceback)

        finally:
            self._task_group = None
            self._closed = True

            if self._pipeline_token is not None:
                _current_pipeline.reset(self._pipeline_token)
                self._pipeline_token = None

            self._discard_unfinished()
            logger.debug(
                'Pipeline closed: %d of %d tasks completed.',
                sum(1 for handle in self._nodes if self._has_result(handle)),
                len(self._nodes),
            )

    def start[T](
        self,
        fn: Callable[..., Awaitable[T]],
        *args: Any,
        task_name: str | None = None,
        depends_on: Iterable[TaskHandle[Any]] = (),
        **kwargs: Any,
    ) -> TaskHandle[T]:
        """
        Start a task, as soon as the handles it depends on have resolved.

        Any `TaskHandle` in `args` or `kwargs.values()` is a dependency, and is replaced by that task's
        result before `fn` is invoked. Only direct arguments are inspected: a handle inside a list, dict
        or any other container is not a dependency and is not resolved.
        Use `depends_on` for tasks to wait on without consuming.

        `task_name` and `depends_on` are keyword-only and belong to the pipeline, so `**kwargs` cannot
        carry arguments of those names through to `fn`. A callable that has parameters called `task_name`
        or `depends_on` takes them through `functools.partial` instead.

        Args:
            fn: async callable to run. Anything returning an awaitable works.
            *args: positional arguments for `fn`. May contain `TaskHandle` instances.
            task_name: task name. Defaults to the callable's name, made unique with a `#N` suffix.
            depends_on: extra handles to wait on without consuming their results.
            **kwargs: keyword arguments for `fn`, except `task_name` and `depends_on`, which the pipeline
                takes for itself. May contain `TaskHandle` instances.

        Raises:
            RuntimeError: if the pipeline is not open.
            ValueError: if `task_name` is taken, or a handle came from another pipeline.

        Returns:
            Handle whose result is the value `fn` returns.
        """
        self._guard_open()

        dependencies = self._collect_dependencies(args, kwargs, depends_on)
        handle: TaskHandle[T] = TaskHandle(self._reserve_name(task_name, fn), owner=self._current_owner())
        handle._future = self._require_loop().create_future()
        node = _Node(handle, fn, args, kwargs, contextvars.copy_context())
        self._nodes[handle] = node

        if any(self._is_settled(dependency) and not self._has_result(dependency) for dependency in dependencies):
            logger.debug('Task %r discarded: a dependency had already failed.', handle.name)
            self._discard(node)
            return handle

        # No await between counting and spawning, so a dependency cannot resolve mid-registration.
        pending = tuple(dependency for dependency in dependencies if not self._is_settled(dependency))

        for dependency in pending:
            self._nodes[dependency].dependents.append(node)

        node.pending_dependencies = len(pending)
        handle._blocked_on = pending
        logger.debug('Started task %r with %d pending dependencies.', handle.name, node.pending_dependencies)

        if node.pending_dependencies == 0:
            self._spawn(node)

        return handle

    def start_many[ItemType, T](
        self,
        fn: Callable[[ItemType], Awaitable[T]],
        inputs: Iterable[ItemType | TaskHandle[ItemType]],
        *,
        task_name: str | None = None,
        depends_on: Iterable[TaskHandle[Any]] = (),
    ) -> TaskHandle[list[T]]:
        """
        Start `fn(item)` once per item in `inputs` and return a barrier handle.

        An item that is a `TaskHandle` is a dependency of its sub-task, and is replaced by that task's
        result. Only the item itself is inspected: a handle inside a list, dict or any other container is
        not a dependency and is not resolved.

        Args:
            fn: async callable invoked once per input.
            inputs: items to fan out over.
            task_name: barrier task name, defaulting to the callable's name. Sub-tasks are named
                `f'{task_name}[{i}]'`,
                so an unnamed fan-out over `fetch` reads `fetch`, `fetch[0]`, `fetch[1]`.
            depends_on: extra handles each sub-task should wait on.

        Raises:
            RuntimeError: if the pipeline is not open.
            ValueError: if any name is taken, or a handle came from another pipeline.

        Returns:
            Barrier handle whose result is the list of sub-task results, in input order.
        """
        self._guard_open()

        items = list(inputs)
        depends_on = tuple(depends_on)

        self._collect_dependencies((), {}, depends_on)
        barrier_name = self._derive_name(task_name, fn)
        sub_names = [f'{barrier_name}[{index}]' for index in range(len(items))]

        for candidate in (barrier_name, *sub_names):
            if candidate in self._names:
                raise ValueError(f"Task name '{candidate}' is already in use.")

        handles = [
            self.start(fn, item, task_name=sub_name, depends_on=depends_on) for sub_name, item in zip(sub_names, items)
        ]
        return self.start(_collect_list, *handles, task_name=barrier_name)

    def gather[T](self, *handles: TaskHandle[T], task_name: str | None = None) -> TaskHandle[list[T]]:
        """
        Collect several tasks into one handle whose result is the list of their results.

        Useful when tasks were started individually — so that each downstream task can depend on just
        the ones it needs — but something also wants them as a single list.

        Args:
            *handles: handles to collect, in the order their results should appear.
            task_name: task name, defaulting to `gather` made unique with a `#N` suffix.

        Raises:
            RuntimeError: if the pipeline is not open.
            ValueError: if `task_name` is taken, or a handle came from another pipeline.

        Returns:
            Handle whose result is the list of the given tasks' results, in argument order.
        """
        self._guard_open()

        return self.start(
            _collect_list,
            *handles,
            task_name=self._derive_name(task_name, _collect_list, base='gather'),
        )

    async def _execute(self, node: _Node) -> None:
        """
        Run one node: claim a permit, invoke the callable, settle its future and release its dependents.

        Args:
            node: internal node record to run.
        """
        handle = node.handle
        future = handle._future
        permit = _Permit(self._require_semaphore(), asyncio.current_task(), handle)

        # Set before the first await. Every node now runs in a copy of its caller's context, so it always
        # inherits that caller's permit, and must never be able to hand back one it does not own.
        permit_token = _current_permit.set(permit)
        pipeline_token = _current_pipeline.set(self)
        unhandled: list[tuple[TaskHandle[Any], BaseException]] = []

        try:
            if future is None:
                raise RuntimeError(f"Task '{handle.name}' has no future.")

            resolved_args = tuple(self._resolve(value) for value in node.args)
            resolved_kwargs = {key: self._resolve(value) for key, value in node.kwargs.items()}
            node.release_arguments()

            await permit.acquire()

            logger.debug('Task %r started.', handle.name)
            started_at = time.perf_counter()

            call_result = node.fn(*resolved_args, **resolved_kwargs)

            if not inspect.isawaitable(call_result):
                wrapped = _unwrap_callable(node.fn)
                callable_name: str = getattr(wrapped, '__qualname__', type(wrapped).__name__)
                raise TypeError(
                    f'{callable_name}() returned {type(call_result).__name__}, not an awaitable; '
                    f'pipeline tasks must be async callables.',
                )

            result = await call_result
            logger.debug('Task %r finished in %.3f s.', handle.name, time.perf_counter() - started_at)

            # A task this one owns failed and nobody read it, so it becomes this task's own failure.
            unhandled = handle._hand_over_owned()

            if unhandled:
                raise unhandled[0][1]

        except asyncio.CancelledError:
            logger.debug('Task %r cancelled.', handle.name)
            handle._hand_over_owned()

            if future is not None and not future.done():
                future.cancel()

            raise

        except Exception as error:
            logger.debug('Task %r failed: %r', handle.name, error)
            owner = handle._owner

            if unhandled and error is unhandled[0][1]:
                error.add_note(f"Not handled by its owner, pipeline task '{handle.name}'.")
            else:
                error.add_note(f"Raised by pipeline task '{handle.name}'.")

                # The task that started this one is gone from the chain, so name it: it is nowhere in
                # the traceback, and it is the first place to look.
                if handle._started_by is not None and (owner is None or owner.name != handle._started_by):
                    error.add_note(
                        f"Task '{handle.name}' was started by '{handle._started_by}', which had already finished.",
                    )

                unhandled = handle._hand_over_owned()

            for _, other in unhandled:
                if other is not error:
                    error.add_note(f"Task '{handle.name}' also left unhandled: {other!r}.")

            if future is not None and not future.done():
                future.set_exception(error)
                # Mark it retrieved: readers take it on demand, and asyncio would otherwise log a bogus
                # "exception was never retrieved" for a failure that is being handled.
                future.exception()

            self._discard_dependents(node)

            # Nobody can handle this: end the pipeline.
            if owner is None:
                raise

            logger.debug('Task %r failure held for its owner %r.', handle.name, owner.name)

        else:
            future.set_result(result)
            self._release_dependents(node)

        finally:
            permit.release()
            _current_permit.reset(permit_token)
            _current_pipeline.reset(pipeline_token)

    def _spawn(self, node: _Node) -> None:
        """
        Turn a node whose dependencies are satisfied into a running asyncio task.

        Args:
            node: internal node record to spawn.
        """
        task_group = self._task_group

        if task_group is None:
            raise RuntimeError('The pipeline is closed.')

        node.handle._blocked_on = ()
        coroutine = self._execute(node)

        try:
            task_group.create_task(coroutine, name=node.handle.name, context=node.context)
        except RuntimeError:
            # Python 3.12's `TaskGroup` doesn't close a refused coroutine (3.13 does); closing it twice is harmless.
            coroutine.close()
            self._discard(node, reason='the pipeline is shutting down')
        else:
            # The task owns the context now, and it holds every `ContextVar` value the starter had.
            node.context = None

    def _release_dependents(self, node: _Node) -> None:
        """
        Count a node's completion against its dependents, spawning the ones that are now ready.

        Args:
            node: internal node record that just completed.
        """
        dependents = node.dependents
        node.dependents = []

        for dependent in dependents:
            dependent.pending_dependencies -= 1

            if dependent.pending_dependencies == 0:
                self._spawn(dependent)

    def _discard_dependents(self, node: _Node) -> None:
        """
        Discard everything downstream of a failure, since none of it can ever run.

        Args:
            node: internal node record that failed.
        """
        dependents = node.dependents
        node.dependents = []

        for dependent in dependents:
            self._discard(dependent)

    def _discard(self, node: _Node, reason: str = 'a dependency did not complete') -> None:
        """
        Discard a node and everything downstream of it.

        Args:
            node: internal node record to discard.
            reason: why it can no longer run, for the log.
        """
        stack = [node]

        while stack:
            discarded = stack.pop()
            future = discarded.handle._future

            if future is not None and not future.done():
                future.cancel()
                logger.debug('Task %r discarded: %s.', discarded.handle.name, reason)

            discarded.release_arguments()
            discarded.context = None
            discarded.handle._blocked_on = ()
            stack.extend(discarded.dependents)
            discarded.dependents = []

    def _discard_unfinished(self) -> None:
        """
        Settle any future still pending once the pipeline has closed, so `result()` reports it accurately.
        """
        for handle in self._nodes:
            future = handle._future

            if future is not None and not future.done():
                future.cancel()

    def _guard_no_wait_cycle(self, waiter: TaskHandle[Any], awaited: TaskHandle[Any]) -> None:
        """
        Raise if waiting on `awaited` would close a cycle, rather than letting the pipeline hang.

        Walks what `awaited` is itself blocked on: the task it is waiting on, or dependencies it has not
        yet received. A `depends_on` edge alone can never form a cycle, but combined with a wait it can.

        Args:
            waiter: handle of the task that is about to wait.
            awaited: handle it is about to wait on.

        Raises:
            RuntimeError: if `waiter` is reachable from `awaited`, so neither could ever finish.
        """
        stack = [awaited]
        seen: set[TaskHandle[Any]] = set()

        while stack:
            handle = stack.pop()

            if handle is waiter:
                raise RuntimeError(
                    f"Task '{waiter.name}' cannot wait on task '{awaited.name}': that would deadlock, "
                    f"because '{awaited.name}' is itself blocked on '{waiter.name}'.",
                )

            if handle in seen or self._is_settled(handle):
                continue

            seen.add(handle)
            stack.extend(blocked for blocked in handle._blocked_on if not self._is_settled(blocked))

    def _current_owner(self) -> TaskHandle[Any] | None:
        """
        Get the handle of the pipeline task calling `start()`, or `None` when called from the scope body.
        """
        permit = _owned_permit()
        return None if permit is None else permit.handle

    def _resolve(self, value: Any) -> Any:
        """
        Substitute a `TaskHandle` for its resolved result; pass other values through.
        """
        if isinstance(value, TaskHandle):
            return value.result()

        return value

    def _has_result(self, handle: TaskHandle[Any]) -> bool:
        """
        Tell whether a handle's task finished with a value, rather than failing or being discarded.
        """
        future = handle._future
        return future is not None and future.done() and not future.cancelled() and future.exception() is None

    def _is_settled(self, handle: TaskHandle[Any]) -> bool:
        """
        Tell whether a handle's task has already finished, one way or another.
        """
        future = handle._future
        return future is not None and future.done()

    def _guard_open(self) -> None:
        """
        Raise unless the pipeline is currently open.
        """
        if self._task_group is None:
            raise RuntimeError('The pipeline is not open; start tasks inside its `async with` block.')

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        """
        Get the event loop the pipeline was opened on.
        """
        if self._loop is None:
            raise RuntimeError('The pipeline is not open.')

        return self._loop

    def _require_semaphore(self) -> asyncio.Semaphore:
        """
        Get the pipeline's concurrency limiter.
        """
        if self._semaphore is None:
            raise RuntimeError('The pipeline is not open.')

        return self._semaphore

    def _reserve_name(self, name: str | None, fn: Callable[..., Awaitable[Any]]) -> str:
        """
        Work out a unique task name and claim it.

        Args:
            name: explicit name, or `None` to derive one from the callable.
            fn: callable the task will run.

        Raises:
            ValueError: if an explicit `name` is already in use.

        Returns:
            The claimed name.
        """
        derived = self._derive_name(name, fn)
        self._claim_name(derived)
        return derived

    def _derive_name(self, name: str | None, fn: Callable[..., Awaitable[Any]], base: str | None = None) -> str:
        """
        Work out a task's name without claiming it, so a caller can build related names from it.

        Args:
            name: explicit name, returned as-is, or `None` to derive one.
            fn: callable the task will run, whose name is used when no `base` is given. A
                `functools.partial` is unwrapped, so the name comes from the function underneath.
            base: stem to derive from instead of the callable's name.

        Returns:
            A name that is free at the moment of the call.
        """
        if name is not None:
            return name

        wrapped = _unwrap_callable(fn)
        stem: str = base if base is not None else getattr(wrapped, '__name__', type(wrapped).__name__)
        index = self._name_counters.get(stem, 0)
        candidate = stem if index == 0 else f'{stem}#{index}'

        while candidate in self._names:
            index += 1
            candidate = f'{stem}#{index}'

        self._name_counters[stem] = index + 1
        return candidate

    def _claim_name(self, name: str) -> None:
        """
        Take a task name, so nothing else can use it.

        Args:
            name: name to claim.

        Raises:
            ValueError: if the name is already in use.
        """
        if name in self._names:
            raise ValueError(f"Task name '{name}' is already in use.")

        self._names.add(name)

    def _collect_dependencies(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        depends_on: Iterable[TaskHandle[Any]],
    ) -> tuple[TaskHandle[Any], ...]:
        """
        Collect deduped dependencies from handle arguments plus `depends_on`.

        Raises:
            ValueError: if a handle was not produced by this pipeline.

        Returns:
            The dependencies, in first-seen order.
        """
        seen: set[TaskHandle[Any]] = set()
        ordered: list[TaskHandle[Any]] = []

        for value in (*args, *kwargs.values(), *depends_on):
            if isinstance(value, TaskHandle):
                if value not in self._nodes:
                    raise ValueError(f"Handle '{value.name}' was not produced by this pipeline.")

                if value not in seen:
                    seen.add(value)
                    ordered.append(value)

        return tuple(ordered)


def current_pipeline() -> TaskPipeline:
    """
    Get the pipeline running in the current task.

    Raises:
        RuntimeError: if no pipeline is open in this task.

    Returns:
        The open pipeline.
    """
    pipeline = _current_pipeline.get()

    if pipeline is None:
        raise RuntimeError('No pipeline is open in this task.')

    return pipeline
