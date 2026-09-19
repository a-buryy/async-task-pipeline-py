# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

### Added

- `TaskPipeline`: an async context manager scope with `start()`, `start_many()`, `gather()` and `results()`.
- `TaskHandle`: a typed handle per task, with `result()`, `done()` and `await wait()`.
- `current_pipeline()`: ambient access to the running pipeline from inside a task.
- Dependency wiring by passing handles as arguments, or through `depends_on`.
- Bounded concurrency via `concurrency_limit`; a task waiting on another hands its permit back.
- Wait-cycle detection, and failure handling decided by task ownership.
- Support for Python 3.12, 3.13 and 3.14.
