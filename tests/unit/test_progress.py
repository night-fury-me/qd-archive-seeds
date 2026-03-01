from __future__ import annotations

from qdarchive_seeding.app.progress import CountersUpdated, ProgressBus, StageChanged


def test_publish_subscribe() -> None:
    bus = ProgressBus()
    events: list[object] = []
    bus.subscribe(events.append)
    bus.publish(StageChanged(stage="extract"))
    assert len(events) == 1
    assert isinstance(events[0], StageChanged)
    assert events[0].stage == "extract"


def test_unsubscribe() -> None:
    bus = ProgressBus()
    events: list[object] = []
    unsub = bus.subscribe(events.append)
    bus.publish(StageChanged(stage="extract"))
    unsub()
    bus.publish(StageChanged(stage="transform"))
    assert len(events) == 1


def test_subscriber_exception_isolated() -> None:
    bus = ProgressBus()
    good_events: list[object] = []

    def bad_callback(event: object) -> None:
        raise RuntimeError("boom")

    bus.subscribe(bad_callback)
    bus.subscribe(good_events.append)
    bus.publish(CountersUpdated(extracted=5))
    assert len(good_events) == 1


def test_multiple_subscribers() -> None:
    bus = ProgressBus()
    a: list[object] = []
    b: list[object] = []
    bus.subscribe(a.append)
    bus.subscribe(b.append)
    bus.publish(StageChanged(stage="done"))
    assert len(a) == 1
    assert len(b) == 1
