"""Suite-wide fixtures.

One stream store serves one client connection, and every test connects its own client. So
each test gets a fresh store, and the memory store starts empty, which each test expects.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests import _streams


@pytest.fixture(autouse=True)
def _stream_store_per_test() -> Iterator[None]:
    yield
    _streams.reset_store()
