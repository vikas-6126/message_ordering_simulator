"""Process layer: P1, P2, P3 keep sequence counters, a vector clock and a Lamport clock."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from .message import Message
from .ordering import CausalBuffer


class Process:
    def __init__(self, pid: str, all_pids: List[str]):
        self.pid = pid
        self.all_pids = list(all_pids)
        self.send_seq: Dict[str, int] = defaultdict(int)       # per-destination (FIFO)
        self.vector_clock: Dict[str, int] = {p: 0 for p in all_pids}   # causal
        self.lamport_clock = 0                                  # total order
        self.delivered_log: List[Message] = []
        # The causal buffer shares the vector clock: VC[k] = messages from k delivered here.
        self.causal = CausalBuffer(self.vector_clock)

    def others(self) -> List[str]:
        return [p for p in self.all_pids if p != self.pid]

    # ---- FIFO ----
    def make_message_fifo(self, receiver: str, content: str) -> Message:
        self.send_seq[receiver] += 1
        return Message(self.pid, receiver, content, seq_no=self.send_seq[receiver])

    # ---- Causal ----
    def make_message_causal(self, receiver: str, content: str) -> Message:
        self.vector_clock[self.pid] += 1     # a send is an event; own messages count as delivered
        return Message(self.pid, receiver, content, vector_clock=dict(self.vector_clock))

    # ---- Total order ----
    def make_message_total(self, receiver: str, content: str) -> Message:
        self.lamport_clock += 1
        return Message(self.pid, receiver, content, lamport_ts=self.lamport_clock)
