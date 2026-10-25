"""Network layer: holds messages in transit and delivers them in a shuffled order."""
from __future__ import annotations

import random
from dataclasses import replace
from typing import List

from .message import Message


class Network:
    """Simulates variable network delay: messages can arrive in any order."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.in_transit: List[Message] = []

    def send(self, message: Message) -> None:
        self.in_transit.append(message)

    def multicast(self, message: Message, receivers: List[str]) -> None:
        """Send one independent copy of `message` to every receiver."""
        for r in receivers:
            self.send(replace(message, receiver=r, vector_clock=dict(message.vector_clock)))

    def flush_to(self, receiver: str, ensure_reordered: bool = True) -> List[Message]:
        """Remove and return everything queued for `receiver` in shuffled arrival order.

        With ensure_reordered=True the arrival order is guaranteed to differ from the
        send order (when there are 2+ messages), so the ordering layer always has
        genuine reordering to repair.
        """
        pending = [m for m in self.in_transit if m.receiver == receiver]
        self.in_transit = [m for m in self.in_transit if m.receiver != receiver]
        original = list(pending)
        self.rng.shuffle(pending)
        if ensure_reordered and len(pending) > 1:
            tries = 0
            while pending == original and tries < 20:
                self.rng.shuffle(pending)
                tries += 1
            if pending == original:
                pending.reverse()
        return pending

    def pop_random(self) -> Message:
        """Deliver one random in-transit message (used by the stress test)."""
        return self.in_transit.pop(self.rng.randrange(len(self.in_transit)))
