Сделаю под проект с эмуляцией ЭКГ, Kafka и ClickHouse, чтобы Codex понимал архитектуру, правила изменений и критерии готовности.

# AGENTS.md

## Project Overview

This repository is a learning and demonstration project for processing ECG
(electrocardiogram) time-series data.

The system simulates an ECG sensor using public ECG datasets, streams samples
through Kafka, processes the signal, and stores raw and aggregated data in
ClickHouse.

The primary goals of the project are:

* experiment with Kafka-based streaming pipelines;
* learn ClickHouse for time-series and analytical workloads;
* implement ECG signal preprocessing and aggregation;
* explore window-based processing such as 500 ms intervals;
* keep the architecture simple enough for experimentation.

This is not a medical system and must not be treated as one.

## High-Level Architecture

Expected data flow:

```text
PhysioNet / WFDB ECG record
          |
          v
    ECG Simulator
          |
          v
        Kafka
          |
          v
    ECG Processor
          |
          +--------------------+
          |                    |
          v                    v
     raw samples          processed data
          |                    |
          +---------+----------+
                    |
                    v
               ClickHouse
                    |
                    v
              analytics/API
```

## Main Components

### ECG Simulator

Responsibilities:

* load ECG recordings using `wfdb`;
* emulate a real sensor according to the source sampling frequency;
* produce ECG samples to Kafka;
* attach timestamps and metadata to every sample;
* optionally replay recordings continuously.

The simulator should preserve the original sampling frequency whenever
possible.

Example message:

```json
{
  "sensor_id": "ecg-001",
  "timestamp": "2026-08-20T12:00:00.123456Z",
  "sample_index": 12345,
  "lead": "MLII",
  "value": -0.145,
  "sampling_rate": 360
}
```

Do not introduce unnecessary batching in the simulator unless batching is an
explicit task requirement.

### Kafka

Kafka is the transport layer between producers and consumers.

Prefer clear topic responsibilities.

Suggested topics:

```text
ecg.raw
ecg.processed
```

Do not add new Kafka topics without a clear reason.

Kafka messages should have stable schemas.

Prefer `sensor_id` as the Kafka message key when message ordering for an
individual sensor matters.

### ECG Processor

Responsibilities may include:

* consuming raw ECG samples;
* validating incoming events;
* buffering samples when required;
* calculating rolling statistics;
* performing smoothing/filtering;
* creating time-window aggregations;
* detecting signal anomalies or peaks when explicitly implemented;
* publishing processed data or writing it to ClickHouse.

Processing code should be separated from Kafka infrastructure code so that
signal-processing functions can be tested independently.

Prefer pure functions for mathematical transformations.

Example:

```python
def moving_average(values: list[float], window_size: int) -> list[float]:
    ...
```

Avoid embedding signal-processing logic directly inside Kafka consumer loops.

## Time Windows

One important experiment in this repository is aggregation by fixed time
windows.

The initial target window is:

```text
500 ms
```

For a 360 Hz signal this corresponds approximately to:

```text
180 samples
```

However, time-based processing should preferably use timestamps rather than
assuming that every window always contains exactly the expected number of
samples.

Possible statistics per window:

* minimum;
* maximum;
* average;
* median;
* standard deviation;
* number of samples.

Do not silently discard samples at window boundaries.

## ClickHouse

ClickHouse stores historical ECG data and analytical aggregates.

Typical tables may include:

```text
ecg_raw
ecg_windows
```

Raw data should preserve enough information to reproduce processing results.

Typical raw columns:

```text
timestamp
sensor_id
lead
sample_index
value
sampling_rate
```

Aggregated rows may contain:

```text
window_start
window_end
sensor_id
lead
sample_count
min_value
max_value
avg_value
stddev_value
```

Prefer `MergeTree`-family table engines unless another engine is justified by
the task.

When designing tables, consider:

* ordering key;
* partitioning;
* timestamp precision;
* expected query patterns;
* volume of raw samples.

Do not use PostgreSQL-style OLTP design patterns blindly in ClickHouse.

Avoid adding indexes unless there is a demonstrated query need.

## Python

Use modern Python.

Preferred version:

```text
Python >= 3.12
```

Use type annotations for public functions and non-trivial internal functions.

Prefer:

```python
def process_sample(sample: ECGSample) -> ProcessedSample:
    ...
```

instead of loosely typed dictionaries passed throughout the application.

Use small domain models where they improve clarity.

Suggested models:

```text
ECGSample
ECGWindow
ProcessedECGWindow
```

Use `dataclass`, Pydantic, or another existing project convention rather than
introducing multiple competing model systems.

## Code Style

Keep code explicit and readable.

Prefer simple solutions over abstractions created for hypothetical future use.

Follow these principles:

* small functions;
* clear names;
* explicit dependencies;
* minimal global state;
* composition over inheritance;
* dependency injection only where it provides practical value;
* no premature framework abstractions.

Avoid classes that only wrap one function without adding state or behavior.

Avoid generic names such as:

```text
Manager
Helper
Utils
Processor
Service
```

unless the responsibility is genuinely clear from context.

Prefer domain-specific names.

## Async Code

Use `asyncio` only for I/O-bound concurrency where it provides a clear
advantage.

Do not convert CPU-bound signal-processing functions to async functions.

Good:

```python
async def consume_messages():
    ...
```

Good:

```python
def calculate_window_statistics(values: Sequence[float]) -> WindowStats:
    ...
```

Avoid unnecessary background tasks.

All created tasks must have a clear lifecycle and error-handling strategy.

## Kafka Consumer Rules

Consumers must explicitly define behavior for:

* deserialization errors;
* invalid messages;
* processing failures;
* retries;
* offset commits;
* graceful shutdown.

Do not commit offsets before data has been successfully processed when this can
cause silent data loss.

Avoid infinite retry loops without backoff.

If retries are implemented, prefer:

```text
exponential backoff + jitter
```

## Error Handling

Do not use:

```python
except Exception:
    pass
```

Do not silently ignore malformed ECG samples.

Errors should provide enough context to identify:

* component;
* sensor;
* message or sample;
* operation that failed.

Catch exceptions at boundaries where recovery is possible.

Allow programming errors to fail visibly during development.

## Logging

Use structured logging where practical.

Important fields include:

```text
sensor_id
topic
partition
offset
sample_index
window_start
window_end
```

Do not log every ECG sample at INFO level in normal operation.

Per-sample logs should be DEBUG-level only.

## Configuration

Configuration must come from environment variables or configuration files.

Do not hard-code:

* Kafka broker addresses;
* ClickHouse credentials;
* database names;
* topic names that need deployment-specific configuration.

Provide sensible development defaults when appropriate.

Never commit secrets.

## Docker

The local development environment should be reproducible through Docker
Compose.

Expected infrastructure may include:

```text
Kafka
ClickHouse
ECG simulator
ECG processor
```

Application containers should remain small and deterministic.

Avoid installing debugging utilities in production images unless needed.

Prefer multi-stage Docker builds when they materially reduce image complexity or
size.

## Dependencies

Before introducing a dependency:

1. check whether the standard library already solves the problem;
2. check whether an existing project dependency provides the functionality;
3. add a new dependency only when it meaningfully simplifies the implementation.

Do not add large frameworks for small utilities.

For ECG datasets, `wfdb` is the preferred library unless there is a concrete
reason to use another implementation.

## Tests

New business logic should normally include tests.

Prioritize tests for:

* message serialization/deserialization;
* window boundaries;
* aggregation;
* filtering;
* missing samples;
* out-of-order samples;
* duplicate samples;
* timestamp handling.

Signal-processing code should be testable without Kafka or ClickHouse.

Prefer unit tests for mathematical transformations and integration tests for
infrastructure boundaries.

Do not make unit tests depend on external PhysioNet availability.

Use small local fixtures for deterministic tests.

## Test Data

ECG data used in tests should be small.

Do not commit large PhysioNet datasets unless explicitly required.

Prefer:

```text
tests/fixtures/
```

with short deterministic samples.

External datasets may be downloaded by development scripts when necessary.

## Database Migrations

Schema changes must be explicit.

When modifying ClickHouse tables:

* preserve existing data where practical;
* document incompatible changes;
* avoid destructive operations unless the task explicitly requires them.

Do not automatically drop production-like tables during application startup.

## Performance

Do not optimize prematurely.

However, remember that ECG streams can generate many rows.

For example:

```text
360 samples/sec
= 21,600 samples/min
= 1,296,000 samples/hour
```

per signal channel.

Therefore pay attention to:

* Kafka message overhead;
* ClickHouse insert batching;
* table ordering;
* aggregation strategy.

Avoid performing one ClickHouse INSERT request per individual ECG sample unless
it is explicitly part of an experiment.

Batch database writes where appropriate.

## Development Commands

Use the project's existing tooling.

Before adding or changing commands, inspect:

```text
pyproject.toml
docker-compose.yml
Makefile
README.md
```

when present.

Typical Python checks should include:

```bash
ruff check .
ruff format --check .
mypy .
pytest
```

If the repository defines different commands, follow repository configuration
instead of these examples.

## Codex Workflow

Before modifying code:

1. inspect the relevant files;
2. understand the current architecture;
3. inspect existing tests;
4. check project configuration;
5. make the smallest coherent change.

Do not rewrite unrelated parts of the repository.

Do not rename public APIs, files, Kafka topics, database tables, or environment
variables unless required by the task.

Preserve backward compatibility where reasonably possible.

When a task is ambiguous, infer intent from existing code and tests before
introducing new architecture.

## When Implementing a Feature

For each feature:

1. identify the component responsible for it;
2. keep domain logic independent from infrastructure where possible;
3. define or update models;
4. implement the smallest working solution;
5. add or update tests;
6. run relevant checks;
7. document externally visible behavior.

## Completion Criteria

Before considering a coding task complete:

* code runs;
* relevant tests pass;
* linting passes;
* type checking passes where configured;
* Docker configuration remains valid if modified;
* no secrets were added;
* no unrelated files were changed;
* new behavior is documented when necessary.

## Important Constraints

This project processes ECG data for educational and engineering experiments.

It is **not** intended to:

* diagnose medical conditions;
* provide medical recommendations;
* replace certified ECG equipment;
* make clinical decisions.

Algorithms such as R-peak detection, arrhythmia classification, filtering, or
anomaly detection should be described as experimental unless they have been
independently validated for another purpose.

Сделаю под проект с эмуляцией ЭКГ, Kafka и ClickHouse, чтобы Codex понимал архитектуру, правила изменений и критерии готовности.

# AGENTS.md

## Project Overview

This repository is a learning and demonstration project for processing ECG
(electrocardiogram) time-series data.

The system simulates an ECG sensor using public ECG datasets, streams samples
through Kafka, processes the signal, and stores raw and aggregated data in
ClickHouse.

The primary goals of the project are:

* experiment with Kafka-based streaming pipelines;
* learn ClickHouse for time-series and analytical workloads;
* implement ECG signal preprocessing and aggregation;
* explore window-based processing such as 500 ms intervals;
* keep the architecture simple enough for experimentation.

This is not a medical system and must not be treated as one.

## High-Level Architecture

Expected data flow:

```text
PhysioNet / WFDB ECG record
          |
          v
    ECG Simulator
          |
          v
        Kafka
          |
          v
    ECG Processor
          |
          +--------------------+
          |                    |
          v                    v
     raw samples          processed data
          |                    |
          +---------+----------+
                    |
                    v
               ClickHouse
                    |
                    v
              analytics/API
```

## Main Components

### ECG Simulator

Responsibilities:

* load ECG recordings using `wfdb`;
* emulate a real sensor according to the source sampling frequency;
* produce ECG samples to Kafka;
* attach timestamps and metadata to every sample;
* optionally replay recordings continuously.

The simulator should preserve the original sampling frequency whenever
possible.

Example message:

```json
{
  "sensor_id": "ecg-001",
  "timestamp": "2026-08-20T12:00:00.123456Z",
  "sample_index": 12345,
  "lead": "MLII",
  "value": -0.145,
  "sampling_rate": 360
}
```

Do not introduce unnecessary batching in the simulator unless batching is an
explicit task requirement.

### Kafka

Kafka is the transport layer between producers and consumers.

Prefer clear topic responsibilities.

Suggested topics:

```text
ecg.raw
ecg.processed
```

Do not add new Kafka topics without a clear reason.

Kafka messages should have stable schemas.

Prefer `sensor_id` as the Kafka message key when message ordering for an
individual sensor matters.

### ECG Processor

Responsibilities may include:

* consuming raw ECG samples;
* validating incoming events;
* buffering samples when required;
* calculating rolling statistics;
* performing smoothing/filtering;
* creating time-window aggregations;
* detecting signal anomalies or peaks when explicitly implemented;
* publishing processed data or writing it to ClickHouse.

Processing code should be separated from Kafka infrastructure code so that
signal-processing functions can be tested independently.

Prefer pure functions for mathematical transformations.

Example:

```python
def moving_average(values: list[float], window_size: int) -> list[float]:
    ...
```

Avoid embedding signal-processing logic directly inside Kafka consumer loops.

## Time Windows

One important experiment in this repository is aggregation by fixed time
windows.

The initial target window is:

```text
500 ms
```

For a 360 Hz signal this corresponds approximately to:

```text
180 samples
```

However, time-based processing should preferably use timestamps rather than
assuming that every window always contains exactly the expected number of
samples.

Possible statistics per window:

* minimum;
* maximum;
* average;
* median;
* standard deviation;
* number of samples.

Do not silently discard samples at window boundaries.

## ClickHouse

ClickHouse stores historical ECG data and analytical aggregates.

Typical tables may include:

```text
ecg_raw
ecg_windows
```

Raw data should preserve enough information to reproduce processing results.

Typical raw columns:

```text
timestamp
sensor_id
lead
sample_index
value
sampling_rate
```

Aggregated rows may contain:

```text
window_start
window_end
sensor_id
lead
sample_count
min_value
max_value
avg_value
stddev_value
```

Prefer `MergeTree`-family table engines unless another engine is justified by
the task.

When designing tables, consider:

* ordering key;
* partitioning;
* timestamp precision;
* expected query patterns;
* volume of raw samples.

Do not use PostgreSQL-style OLTP design patterns blindly in ClickHouse.

Avoid adding indexes unless there is a demonstrated query need.

## Python

Use modern Python.

Preferred version:

```text
Python >= 3.12
```

Use type annotations for public functions and non-trivial internal functions.

Prefer:

```python
def process_sample(sample: ECGSample) -> ProcessedSample:
    ...
```

instead of loosely typed dictionaries passed throughout the application.

Use small domain models where they improve clarity.

Suggested models:

```text
ECGSample
ECGWindow
ProcessedECGWindow
```

Use `dataclass`, Pydantic, or another existing project convention rather than
introducing multiple competing model systems.

## Code Style

Keep code explicit and readable.

Prefer simple solutions over abstractions created for hypothetical future use.

Follow these principles:

* small functions;
* clear names;
* explicit dependencies;
* minimal global state;
* composition over inheritance;
* dependency injection only where it provides practical value;
* no premature framework abstractions.

Avoid classes that only wrap one function without adding state or behavior.

Avoid generic names such as:

```text
Manager
Helper
Utils
Processor
Service
```

unless the responsibility is genuinely clear from context.

Prefer domain-specific names.

## Async Code

Use `asyncio` only for I/O-bound concurrency where it provides a clear
advantage.

Do not convert CPU-bound signal-processing functions to async functions.

Good:

```python
async def consume_messages():
    ...
```

Good:

```python
def calculate_window_statistics(values: Sequence[float]) -> WindowStats:
    ...
```

Avoid unnecessary background tasks.

All created tasks must have a clear lifecycle and error-handling strategy.

## Kafka Consumer Rules

Consumers must explicitly define behavior for:

* deserialization errors;
* invalid messages;
* processing failures;
* retries;
* offset commits;
* graceful shutdown.

Do not commit offsets before data has been successfully processed when this can
cause silent data loss.

Avoid infinite retry loops without backoff.

If retries are implemented, prefer:

```text
exponential backoff + jitter
```

## Error Handling

Do not use:

```python
except Exception:
    pass
```

Do not silently ignore malformed ECG samples.

Errors should provide enough context to identify:

* component;
* sensor;
* message or sample;
* operation that failed.

Catch exceptions at boundaries where recovery is possible.

Allow programming errors to fail visibly during development.

## Logging

Use structured logging where practical.

Important fields include:

```text
sensor_id
topic
partition
offset
sample_index
window_start
window_end
```

Do not log every ECG sample at INFO level in normal operation.

Per-sample logs should be DEBUG-level only.

## Configuration

Configuration must come from environment variables or configuration files.

Do not hard-code:

* Kafka broker addresses;
* ClickHouse credentials;
* database names;
* topic names that need deployment-specific configuration.

Provide sensible development defaults when appropriate.

Never commit secrets.

## Docker

The local development environment should be reproducible through Docker
Compose.

Expected infrastructure may include:

```text
Kafka
ClickHouse
ECG simulator
ECG processor
```

Application containers should remain small and deterministic.

Avoid installing debugging utilities in production images unless needed.

Prefer multi-stage Docker builds when they materially reduce image complexity or
size.

## Dependencies

Before introducing a dependency:

1. check whether the standard library already solves the problem;
2. check whether an existing project dependency provides the functionality;
3. add a new dependency only when it meaningfully simplifies the implementation.

Do not add large frameworks for small utilities.

For ECG datasets, `wfdb` is the preferred library unless there is a concrete
reason to use another implementation.

## Tests

New business logic should normally include tests.

Prioritize tests for:

* message serialization/deserialization;
* window boundaries;
* aggregation;
* filtering;
* missing samples;
* out-of-order samples;
* duplicate samples;
* timestamp handling.

Signal-processing code should be testable without Kafka or ClickHouse.

Prefer unit tests for mathematical transformations and integration tests for
infrastructure boundaries.

Do not make unit tests depend on external PhysioNet availability.

Use small local fixtures for deterministic tests.

## Test Data

ECG data used in tests should be small.

Do not commit large PhysioNet datasets unless explicitly required.

Prefer:

```text
tests/fixtures/
```

with short deterministic samples.

External datasets may be downloaded by development scripts when necessary.

## Database Migrations

Schema changes must be explicit.

When modifying ClickHouse tables:

* preserve existing data where practical;
* document incompatible changes;
* avoid destructive operations unless the task explicitly requires them.

Do not automatically drop production-like tables during application startup.

## Performance

Do not optimize prematurely.

However, remember that ECG streams can generate many rows.

For example:

```text
360 samples/sec
= 21,600 samples/min
= 1,296,000 samples/hour
```

per signal channel.

Therefore pay attention to:

* Kafka message overhead;
* ClickHouse insert batching;
* table ordering;
* aggregation strategy.

Avoid performing one ClickHouse INSERT request per individual ECG sample unless
it is explicitly part of an experiment.

Batch database writes where appropriate.

## Development Commands

Use the project's existing tooling.

Before adding or changing commands, inspect:

```text
pyproject.toml
docker-compose.yml
Makefile
README.md
```

when present.

Typical Python checks should include:

```bash
ruff check .
ruff format --check .
mypy .
pytest
```

If the repository defines different commands, follow repository configuration
instead of these examples.

## Codex Workflow

Before modifying code:

1. inspect the relevant files;
2. understand the current architecture;
3. inspect existing tests;
4. check project configuration;
5. make the smallest coherent change.

Do not rewrite unrelated parts of the repository.

Do not rename public APIs, files, Kafka topics, database tables, or environment
variables unless required by the task.

Preserve backward compatibility where reasonably possible.

When a task is ambiguous, infer intent from existing code and tests before
introducing new architecture.

## When Implementing a Feature

For each feature:

1. identify the component responsible for it;
2. keep domain logic independent from infrastructure where possible;
3. define or update models;
4. implement the smallest working solution;
5. add or update tests;
6. run relevant checks;
7. document externally visible behavior.

## Completion Criteria

Before considering a coding task complete:

* code runs;
* relevant tests pass;
* linting passes;
* type checking passes where configured;
* Docker configuration remains valid if modified;
* no secrets were added;
* no unrelated files were changed;
* new behavior is documented when necessary.

## Important Constraints

This project processes ECG data for educational and engineering experiments.

It is **not** intended to:

* diagnose medical conditions;
* provide medical recommendations;
* replace certified ECG equipment;
* make clinical decisions.

Algorithms such as R-peak detection, arrhythmia classification, filtering, or
anomaly detection should be described as experimental unless they have been
independently validated for another purpose.

Я бы добавил это прямо в раздел **Codex Workflow**, чтобы Codex не запускал `pytest`, `mypy` и `ruff` после каждой мелкой правки.

## Validation During Development

Do not run the entire test suite, linter, formatter, and type checker after every
small code change.

Use incremental validation during development.

After a small change:

* run only the directly affected test or test module when practical;
* run a targeted linter or type check only when it is useful for the change;
* avoid repeatedly running expensive project-wide checks while actively editing.

For example, prefer:

```bash
pytest tests/test_window.py
```

or:

```bash
pytest tests/test_window.py::test_500ms_window
```

instead of repeatedly running:

```bash
pytest
ruff check .
mypy .
```

For trivial changes such as comments, documentation, logging text, or obvious
local refactoring, running tests after every edit is usually unnecessary.

Run broader checks when:

* a logical unit of work is complete;
* shared interfaces or models have changed;
* the change affects multiple components;
* targeted tests indicate a possible regression;
* the task is ready for final validation.

Before considering the task complete, run the relevant broader checks required
by the project's completion criteria.

The goal is to maintain correctness without wasting development time by
repeatedly running the same expensive checks after every edit.

Да. Я бы добавил в `AGENTS.md` отдельный раздел **Documentation Style**. Google Python Style Guide требует `"""` для docstrings, однострочное summary и, когда необходимо, секции `Args:`, `Returns:`/`Yields:` и `Raises:`. При этом docstring обязателен прежде всего для public API, нетривиальных функций и функций с неочевидной логикой — не нужно заставлять Codex документировать каждый тривиальный private-метод. ([Google GitHub][1])

## Documentation Style

All source code documentation must be written in English.

This includes:

* docstrings;
* code comments;
* TODO comments;
* public API documentation;
* descriptions of classes, methods, functions, and modules.

Write Python docstrings according to the Google Python Style Guide.

Use triple double quotes:

```python
"""Load an ECG record from the configured data source."""
```

Add docstrings for:

* public classes;
* public functions and methods;
* modules when a module-level description provides useful context;
* non-trivial private functions or methods;
* functions whose behavior is not obvious from their name and signature.

Do not add redundant docstrings to trivial private functions when the name,
signature, and implementation are already self-explanatory.

For non-trivial functions, use Google-style sections where applicable:

```python
def load_ecg_record(
    record_name: str,
    start_sample: int,
    end_sample: int,
) -> ECGRecord:
    """Load a range of samples from an ECG record.

    Args:
        record_name: Name of the ECG record to load.
        start_sample: Index of the first sample to load.
        end_sample: Index after the last sample to load.

    Returns:
        The ECG record containing the requested samples.

    Raises:
        ValueError: If the requested sample range is invalid.
        ECGDataError: If the ECG record cannot be loaded.
    """
```

Use the following sections when appropriate:

* `Args:` for function and method arguments;
* `Returns:` for returned values;
* `Yields:` for generators;
* `Raises:` for exceptions that are part of the function's interface.

Do not repeat type annotations unnecessarily in docstrings.

Prefer:

```python
def calculate_average(values: Sequence[float]) -> float:
    """Calculate the average ECG signal value.

    Args:
        values: ECG signal values.

    Returns:
        The arithmetic mean of the values.
    """
```

Avoid:

```python
def calculate_average(values: Sequence[float]) -> float:
    """Calculate average.

    Args:
        values (Sequence[float]): Sequence of floats.

    Returns:
        float: Float value.
    """
```

Docstrings should describe behavior and semantics rather than restating the
implementation.

Keep the summary line concise and end it with punctuation.

Use comments to explain why something is done when the reason is not obvious.
Do not use comments merely to translate the code into English.

Prefer:

```python
# Use timestamps instead of sample counts because samples may arrive late.
window = create_window(samples)
```

Avoid:

```python
# Create a window.
window = create_window(samples)
```

When modifying existing Python code, preserve the Google docstring style and
update affected docstrings if the public behavior changes.

