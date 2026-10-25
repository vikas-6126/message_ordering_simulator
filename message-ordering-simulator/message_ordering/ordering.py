"""Ordering layer: FIFO, causal and total-order delivery buffers (report section 6)."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Dict, List

from .message import Message


class FifoBuffer:
    """6.1 - per-sender sequence numbers; release message n only after 1..n-1."""

    def __init__(self):
        self.buffers: Dict[str, Dict[int, Message]] = defaultdict(dict)
        self.expected: Dict[str, int] = defaultdict(lambda: 1)
        self.trace: List[str] = []

    @property
    def pending(self) -> int:
        return sum(len(b) for b in self.buffers.values())

    def receive(self, msg: Message) -> List[Message]:
        s = msg.sender
        if msg.seq_no < self.expected[s] or msg.seq_no in self.buffers[s]:
            self.trace.append(f"IGNORE duplicate {msg!r}")
            return []
        self.buffers[s][msg.seq_no] = msg
        if msg.seq_no != self.expected[s]:
            self.trace.append(f"BUFFER  {msg!r}  (expected #{self.expected[s]} from {s} first)")
        delivered = []
        while self.expected[s] in self.buffers[s]:
            m = self.buffers[s].pop(self.expected[s])
            delivered.append(m)
            self.trace.append(f"DELIVER {m!r}")
            self.expected[s] += 1
        return delivered


class CausalBuffer:
    """6.2 - vector-clock dependency check (Birman-Schiper-Stephenson).

    `delivered_vc[k]` = number of messages from k already delivered at this process.
    A message m from s is deliverable iff
        m.vc[s] == delivered_vc[s] + 1   and   m.vc[k] <= delivered_vc[k] for all k != s.
    """

    def __init__(self, delivered_vc: Dict[str, int]):
        self.delivered_vc = delivered_vc
        self.pending: List[Message] = []
        self.trace: List[str] = []

    def _unmet(self, m: Message) -> List[str]:
        s, vc = m.sender, self.delivered_vc
        bad = [k for k, v in m.vector_clock.items() if k != s and v > vc[k]]
        if m.vector_clock[s] > vc[s] + 1:
            bad.append(s)
        return bad

    def can_deliver(self, m: Message) -> bool:
        return m.vector_clock[m.sender] == self.delivered_vc[m.sender] + 1 and not self._unmet(m)

    def receive(self, msg: Message) -> List[Message]:
        if msg.vector_clock[msg.sender] <= self.delivered_vc[msg.sender]:
            self.trace.append(f"IGNORE duplicate {msg!r}")
            return []
        if not self.can_deliver(msg):
            self.trace.append(f"BUFFER  {msg!r}  (waiting for earlier message(s) from "
                              f"{', '.join(self._unmet(msg))})")
        self.pending.append(msg)
        delivered: List[Message] = []
        progress = True
        while progress:                       # a delivery may unblock buffered messages
            progress = False
            for m in list(self.pending):
                if self.can_deliver(m):
                    self.pending.remove(m)
                    self.delivered_vc[m.sender] += 1
                    delivered.append(m)
                    self.trace.append(f"DELIVER {m!r}")
                    progress = True
        return delivered


class Sequencer:
    """6.3 - central process that stamps every message with the next global number."""

    def __init__(self):
        self.counter = 0

    def stamp(self, msg: Message) -> Message:
        self.counter += 1
        return replace(msg, global_seq=self.counter)


class TotalOrderBuffer:
    """6.3 - release messages strictly in increasing global sequence number."""

    def __init__(self):
        self.buffer: Dict[int, Message] = {}
        self.next_expected = 1
        self.trace: List[str] = []

    @property
    def pending(self) -> int:
        return len(self.buffer)

    def receive(self, msg: Message) -> List[Message]:
        self.buffer[msg.global_seq] = msg
        if msg.global_seq != self.next_expected:
            self.trace.append(f"BUFFER  {msg!r}  (next expected G={self.next_expected})")
        delivered = []
        while self.next_expected in self.buffer:
            m = self.buffer.pop(self.next_expected)
            delivered.append(m)
            self.trace.append(f"DELIVER {m!r}")
            self.next_expected += 1
        return delivered
