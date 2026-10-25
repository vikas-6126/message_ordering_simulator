"""The three simulations of the report, plus a causal-order stress test."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List

from .checks import is_causal, is_fifo, is_total
from .message import Message
from .network import Network
from .ordering import FifoBuffer, Sequencer, TotalOrderBuffer
from .process import Process

PIDS = ["P1", "P2", "P3"]


@dataclass
class ScenarioResult:
    name: str
    arrival: Dict[str, List[Message]]
    delivered: Dict[str, List[Message]]
    pending: Dict[str, int]
    ok: bool
    trace: Dict[str, List[str]] = field(default_factory=dict)


def _fmt(msgs: List[Message]) -> str:
    return ", ".join(repr(m) for m in msgs) if msgs else "(none)"


def _say(verbose: bool):
    return print if verbose else (lambda *a, **k: None)


def _make_procs() -> Dict[str, Process]:
    return {p: Process(p, PIDS) for p in PIDS}


# ----------------------------------------------------------------------
# 1. FIFO ordering
# ----------------------------------------------------------------------
def simulate_fifo_ordering(seed: int = 7, verbose: bool = True) -> ScenarioResult:
    say = _say(verbose)
    say("\n===== FIFO ORDERING SIMULATION =====")
    procs, net = _make_procs(), Network(seed)

    for text in ["A1", "A2", "A3"]:                   # P1 -> P3 : 3 messages
        net.send(procs["P1"].make_message_fifo("P3", text))
    for text in ["B1", "B2"]:                         # P2 -> P3 : 2 messages
        net.send(procs["P2"].make_message_fifo("P3", text))

    arrived = net.flush_to("P3")
    say("Arrival order at network layer (out of order):", _fmt(arrived))

    buf, delivered = FifoBuffer(), []
    for m in arrived:
        delivered.extend(buf.receive(m))
    say("Buffer activity at P3:")
    for line in buf.trace:
        say("   ", line)
    say("FIFO delivery order at P3          :", _fmt(delivered))
    ok = is_fifo(delivered) and len(delivered) == len(arrived)
    say("Per-sender FIFO order respected    :", ok)
    return ScenarioResult("fifo", {"P3": arrived}, {"P3": delivered},
                          {"P3": buf.pending}, ok, {"P3": buf.trace})


# ----------------------------------------------------------------------
# 2. Causal ordering
# ----------------------------------------------------------------------
def simulate_causal_ordering(seed: int = 3, verbose: bool = True) -> ScenarioResult:
    """P1 multicasts m1; after P2 DELIVERS m1 it multicasts m2 (m2 depends on m1).
    The network hands m2 to P3 before m1; P3 must still deliver m1 first."""
    say = _say(verbose)
    say("\n===== CAUSAL ORDERING SIMULATION =====")
    procs, net = _make_procs(), Network(seed)
    logs: Dict[str, List[Message]] = {p: [] for p in PIDS}

    m1 = procs["P1"].make_message_causal("ALL", "REQUEST")
    logs["P1"].append(m1)
    net.multicast(m1, procs["P1"].others())

    # P2 receives m1 and delivers it (updating its vector clock) ...
    for m in net.flush_to("P2"):
        logs["P2"].extend(procs["P2"].causal.receive(m))
    # ... and only then sends m2, which therefore causally depends on m1.
    m2 = procs["P2"].make_message_causal("ALL", "FORWARD(REQUEST)")
    logs["P2"].append(m2)
    net.multicast(m2, procs["P2"].others())

    for m in net.flush_to("P1"):                       # P1 receives m2
        logs["P1"].extend(procs["P1"].causal.receive(m))

    arrived = net.flush_to("P3")                       # P3 sees m2 BEFORE m1
    say("Sent: m1 =", repr(m1), "| m2 =", repr(m2))
    say("Arrival order at network layer (out of order):", _fmt(arrived))
    for m in arrived:
        logs["P3"].extend(procs["P3"].causal.receive(m))
    say("Buffer activity at P3:")
    for line in procs["P3"].causal.trace:
        say("   ", line)
    say("Causal delivery order at P3        :", _fmt(logs["P3"]))
    pend = {p: len(procs[p].causal.pending) for p in PIDS}
    say("Messages still buffered (waiting)  :", pend["P3"])
    ok = is_causal(logs) and all(v == 0 for v in pend.values())
    say("Causal order respected everywhere  :", ok)
    return ScenarioResult("causal", {"P3": arrived}, logs, pend, ok,
                          {"P3": procs["P3"].causal.trace})


def simulate_concurrent_causal(seed: int = 5, verbose: bool = True) -> ScenarioResult:
    """TC04: two causally UNRELATED multicasts may be delivered in either order."""
    say = _say(verbose)
    say("\n===== CAUSAL ORDERING: CONCURRENT MESSAGES =====")
    procs, net = _make_procs(), Network(seed)
    c1 = procs["P1"].make_message_causal("ALL", "C1 (from P1)")
    c2 = procs["P2"].make_message_causal("ALL", "C2 (from P2)")     # P2 never saw c1
    net.multicast(c1, ["P3"])
    net.multicast(c2, ["P3"])
    arrived = net.flush_to("P3")
    delivered: List[Message] = []
    for m in arrived:
        delivered.extend(procs["P3"].causal.receive(m))
    say("Arrival order at P3 :", _fmt(arrived))
    say("Delivery order at P3:", _fmt(delivered))
    say("Nothing needed buffering (messages are concurrent):", not procs["P3"].causal.pending)
    ok = len(delivered) == 2 and not procs["P3"].causal.pending
    return ScenarioResult("concurrent", {"P3": arrived}, {"P3": delivered},
                          {"P3": len(procs["P3"].causal.pending)}, ok,
                          {"P3": procs["P3"].causal.trace})


# ----------------------------------------------------------------------
# 3. Total ordering (central sequencer)
# ----------------------------------------------------------------------
def simulate_total_ordering(seed: int = 21, verbose: bool = True) -> ScenarioResult:
    say = _say(verbose)
    say("\n===== TOTAL ORDERING SIMULATION =====")
    procs, net, seq = _make_procs(), Network(seed), Sequencer()

    # Every process sends its update to the sequencer.
    for pid, text in [("P1", "UPDATE_X"), ("P2", "UPDATE_Y"), ("P3", "UPDATE_Z")]:
        net.send(procs[pid].make_message_total("SEQ", text))
    at_sequencer = net.flush_to("SEQ")
    say("Arrival order at the Sequencer:", _fmt(at_sequencer))

    # The sequencer stamps in arrival order and multicasts to EVERY process.
    for m in at_sequencer:
        net.multicast(seq.stamp(m), PIDS)

    arrival: Dict[str, List[Message]] = {}
    delivered: Dict[str, List[Message]] = {}
    buffers = {p: TotalOrderBuffer() for p in PIDS}
    for pid in PIDS:
        arrival[pid] = net.flush_to(pid)               # different shuffle per receiver
        delivered[pid] = []
        say(f"Arrival order at {pid} (network layer): {_fmt(arrival[pid])}")
        for m in arrival[pid]:
            delivered[pid].extend(buffers[pid].receive(m))
    for pid in PIDS:
        say(f"Total-order delivery at {pid}        : {_fmt(delivered[pid])}")
    ok = is_total(delivered) and all(len(d) == 3 for d in delivered.values())
    say("Same global order at every process :", ok)
    return ScenarioResult("total", arrival, delivered,
                          {p: buffers[p].pending for p in PIDS}, ok,
                          {p: buffers[p].trace for p in PIDS})


# ----------------------------------------------------------------------
# Stress test: many processes, random sends and random network delivery
# ----------------------------------------------------------------------
def stress_causal(num_processes: int = 4, num_messages: int = 30, seed: int = 0) -> ScenarioResult:
    rng = random.Random(seed)
    pids = [f"P{i}" for i in range(1, num_processes + 1)]
    procs = {p: Process(p, pids) for p in pids}
    net = Network(seed)
    logs: Dict[str, List[Message]] = {p: [] for p in pids}
    sent = 0
    while sent < num_messages or net.in_transit:
        if sent < num_messages and (not net.in_transit or rng.random() < 0.4):
            p = rng.choice(pids)
            m = procs[p].make_message_causal("ALL", f"m{sent + 1}")
            sent += 1
            logs[p].append(m)
            net.multicast(m, procs[p].others())
        else:
            m = net.pop_random()
            logs[m.receiver].extend(procs[m.receiver].causal.receive(m))
    pend = {p: len(procs[p].causal.pending) for p in pids}
    ok = is_causal(logs) and all(len(l) == num_messages for l in logs.values()) \
        and all(v == 0 for v in pend.values())
    return ScenarioResult("stress-causal", {}, logs, pend, ok)
