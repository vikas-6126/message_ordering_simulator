"""The Message class (report section 7.1)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(eq=False)  # identity equality: two copies of a multicast are distinct messages
class Message:
    sender: str
    receiver: str
    content: str
    seq_no: int = 0                                   # per-sender, per-destination FIFO number
    vector_clock: Dict[str, int] = field(default_factory=dict)   # causal ordering
    lamport_ts: Optional[int] = None                  # Lamport timestamp at the sender
    global_seq: Optional[int] = None                  # assigned by the Sequencer (total order)

    def __repr__(self) -> str:
        parts = [f"{self.sender}->{self.receiver}"]
        if self.seq_no:
            parts.append(f"#{self.seq_no}")
        if self.vector_clock:
            parts.append("VC=[" + ",".join(str(v) for v in self.vector_clock.values()) + "]")
        if self.global_seq is not None:
            parts.append(f"G={self.global_seq}")
        parts.append(f"'{self.content}'")
        return "[" + " ".join(parts) + "]"
