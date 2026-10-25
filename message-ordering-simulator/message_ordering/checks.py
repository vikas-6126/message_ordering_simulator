"""Independent verifiers: do the delivery logs really satisfy each guarantee?"""
from __future__ import annotations

from typing import Dict, List

from .message import Message


def is_fifo(delivered: List[Message]) -> bool:
    """Per sender, sequence numbers appear in increasing order."""
    last: Dict[str, int] = {}
    for m in delivered:
        if m.seq_no <= last.get(m.sender, 0):
            return False
        last[m.sender] = m.seq_no
    return True


def happened_before(a: Message, b: Message) -> bool:
    """send(a) -> send(b) iff VC(a) <= VC(b) component-wise and VC(a) != VC(b)."""
    return a.vector_clock != b.vector_clock and all(
        a.vector_clock[k] <= b.vector_clock[k] for k in a.vector_clock)


def is_causal(logs: Dict[str, List[Message]]) -> bool:
    """No process delivers a message before one that causally precedes it."""
    for log in logs.values():
        for i in range(len(log)):
            for j in range(i + 1, len(log)):
                if happened_before(log[j], log[i]):
                    return False
    return True


def is_total(logs: Dict[str, List[Message]]) -> bool:
    """Every process delivered exactly the same sequence."""
    seqs = [[(m.sender, m.content) for m in log] for log in logs.values()]
    return all(s == seqs[0] for s in seqs)
