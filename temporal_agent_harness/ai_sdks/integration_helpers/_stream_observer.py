"""Live-streaming observer contract shared across AI-SDK Temporal integrations.

A *streamed* model/agent call collects all chunks in the activity and returns
them batched for the workflow to parse.  Independently, the activity can hand
each raw chunk to a :class:`StreamObserver` as it arrives, so external consumers
see the output live.

This contract is deliberately SDK-agnostic: ``RawEvent`` is a type variable and
the per-call routing token is opaque (``Any``).  An SDK plugin's streamed
activity never type-switches on the token — it always hands the token to a
configured :class:`ObserverFactory`, which returns a fresh observer per call.
The same contract serves the Gemini, OpenAI-agents, and any future plugin, so
each plugin's "publish raw events to a workflow stream" path is one shared
abstraction rather than N divergent re-implementations.
"""

from __future__ import annotations

from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
)
from datetime import timedelta
from typing import Any, AsyncIterator, Optional, Protocol, TypeVar

from pydantic import BaseModel, ValidationError
from temporalio.contrib.streams import ActivityStream, BufferedPublisher

__all__ = [
    "StreamObserver",
    "ObserverFactory",
    "RawTopic",
    "RawTopicObserver",
    "select_observer",
]

RawEvent = TypeVar("RawEvent", contravariant=True)

# Fallback publish-flush cadence, used only when a caller does not pass one. Every
# plugin-driven call DOES pass one (the OpenAI plugin threads
# ``ModelActivityParameters.streaming_batch_interval``), so this governs only direct
# construction; it mirrors that field's own default so the two never disagree.
_DEFAULT_BATCH_INTERVAL = timedelta(milliseconds=100)


class StreamObserver(Protocol[RawEvent]):
    """Consumes the raw provider events of ONE streamed call, live, in the activity.

    An async context manager: ``__aenter__`` acquires any sink resource (e.g. a
    stream producer), ``on_event`` is called once per raw event in
    arrival order, and ``__aexit__`` releases — receiving the exception if the
    stream errored.
    """

    async def __aenter__(self) -> StreamObserver[RawEvent]: ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None: ...

    async def on_event(self, event: RawEvent) -> None: ...


class ObserverFactory(Protocol):
    """Per-call factory: an opaque routing token -> a fresh observer context manager.

    Per-call state (arg buffers, tool brackets, the stream publisher) lives
    inside the returned observer, so concurrent streamed calls on a shared worker
    never bleed state into one another.

    ``batch_interval`` is the publish-flush cadence the *plugin* was configured with
    (``ModelActivityParameters.streaming_batch_interval``), handed to every factory so
    a custom observer's own publisher honors the same setting as the built-in
    :class:`RawTopicObserver` — the alternative being a configured interval that
    silently does nothing whenever a factory is wired. Keyword-only, so a factory
    that ignores cadence can still declare it as ``**_kw``.
    """

    def __call__(
        self, token: Any, *, batch_interval: timedelta
    ) -> AbstractAsyncContextManager["StreamObserver[Any]"]: ...


class RawTopic(BaseModel):
    """The routing token of the default observer: a Stream and the Topic to publish on.

    An activity gets a Stream only in its input, so the workflow side names it here, for
    example from a ``stream_to_provider``. The token rides the activity input as plain JSON,
    and :func:`select_observer` validates it back.
    """

    stream: ActivityStream[Any]
    topic: str


class RawTopicObserver:
    """Default observer: publish each raw event onto a topic of a Stream.

    Reproduces the SDK plugins' current streaming behavior, generalized to any
    event type: open a publisher from within the activity, then publish every
    event handed to :meth:`on_event` to a single named topic so external
    consumers observe the stream live.  Publishing is best-effort — a malformed
    or unserializable event is dropped rather than breaking the batched-collect
    path the workflow ultimately parses.
    """

    def __init__(
        self,
        target: RawTopic,
        *,
        event_type: type | None = None,
        batch_interval: timedelta = _DEFAULT_BATCH_INTERVAL,
    ) -> None:
        self._target = target
        self._event_type = event_type
        self._batch_interval = batch_interval
        self._publisher: BufferedPublisher[Any] | None = None

    async def __aenter__(self) -> RawTopicObserver:
        topic = (
            self._target.stream.topic(self._target.topic, type=self._event_type)
            if self._event_type is not None
            else self._target.stream.topic(self._target.topic)
        )
        self._publisher = BufferedPublisher(topic, max_delay=self._batch_interval)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        publisher, self._publisher = self._publisher, None
        if publisher is not None:
            # The tail goes out also when the call failed, so a reader sees the events up to
            # the failure.
            await publisher.__aexit__(exc_type, exc, tb)
        # Never swallow a stream error: the batched-collect path owns failures.
        return False

    async def on_event(self, event: Any) -> None:
        if self._publisher is None:
            return
        try:
            self._publisher.publish(event)
        except Exception:
            # Best-effort, mirrors the current activity behavior: a bad event
            # must not break the batched return the workflow parses.
            pass


@asynccontextmanager
async def _null_observer() -> AsyncIterator[None]:
    """An async-CM that yields ``None`` — the "no live observer" sentinel.

    Callers ``async with select_observer(...) as obs`` and skip ``on_event``
    when ``obs is None``, so a streamed call with no routing token (or a
    non-``str`` token and no factory) runs exactly as it did before observers.
    """
    yield None


def select_observer(
    *,
    factory: Optional[ObserverFactory],
    token: Any,
    event_type: type | None = None,
    batch_interval: timedelta = _DEFAULT_BATCH_INTERVAL,
) -> AbstractAsyncContextManager[Optional[StreamObserver[Any]]]:
    """Resolve the per-call live observer from the routing token.

    - ``token is None`` → no observer (the factory is never called).
    - a ``factory`` is configured → ``factory(token, batch_interval=...)`` (the plugin
      hands the opaque token straight through; it never type-switches on it, and passes
      the configured flush cadence so a custom observer's publisher can honor it).
    - no factory but a :class:`RawTopic` token (or its JSON) → the default
      :class:`RawTopicObserver` on that Stream and Topic.
    - no factory but a ``str`` token → ``ValueError``: a bare topic name names no
      Stream, and dropping the events would hide that.
    - no factory and any other token → no observer (a non-default token with
      nothing configured to consume it is a no-op rather than an error).
    """
    if token is None:
        return _null_observer()
    if factory is not None:
        return factory(token, batch_interval=batch_interval)
    if isinstance(token, str):
        raise ValueError(
            f"The streaming topic {token!r} names no Stream. Return a RawTopic with the "
            "Stream from stream_to_provider."
        )
    target = _raw_topic(token)
    if target is not None:
        return RawTopicObserver(
            target, event_type=event_type, batch_interval=batch_interval
        )
    return _null_observer()


def _raw_topic(token: Any) -> RawTopic | None:
    if isinstance(token, RawTopic):
        return token
    if not isinstance(token, dict) or set(token) != {"stream", "topic"}:
        return None
    try:
        return RawTopic.model_validate(token)
    except ValidationError:
        return None
