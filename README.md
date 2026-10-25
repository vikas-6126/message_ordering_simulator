# Message-Ordering Simulator

**Design and Implementation of FIFO, Causal, and Total Ordering of Messages**
Distributed Systems project, 2026–2027.

In a distributed system the network can deliver messages in a different order from the one in
which they were sent. This simulator models a network that deliberately reorders messages, then
shows how an *ordering layer* at the receiver buffers and releases them so that each guarantee holds.

| Guarantee | Mechanism | Code |
|---|---|---|
| **FIFO** – messages from one sender are delivered in send order | per-sender sequence numbers + hold-back buffer | `FifoBuffer` |
| **Causal** – if send(m1) happened-before send(m2), everyone delivers m1 first | vector clocks, delivery condition (Birman–Schiper–Stephenson) | `CausalBuffer` |
| **Total** – every process delivers every message in the same order | central sequencer + global sequence numbers | `Sequencer`, `TotalOrderBuffer` |

## Architecture (Process → Network → Ordering → Application)

```
Process layer    P1, P2, P3   (sequence counters, vector clock, Lamport clock)
      |
Network layer    Network      (keeps messages in transit, shuffles arrival order)
      |
Ordering layer   FifoBuffer | CausalBuffer | TotalOrderBuffer
      |
Delivery to application
```

## Run

Python 3.9+, no third-party packages.

```bash
python message_ordering_simulator.py            # all simulations
python message_ordering_simulator.py fifo       # FIFO only   (also: causal, total)
python message_ordering_simulator.py causal --seed 3
python message_ordering_simulator.py --stress   # randomised causal-order stress test
python -m unittest discover -v                  # unit tests
```

## Sample output (excerpt)

```
---------- MESSAGE ORDERING SIMULATOR ----------

===== CAUSAL ORDERING SIMULATION =====
Sent: m1 = [P1->ALL VC=[1,0,0] 'REQUEST'] | m2 = [P2->ALL VC=[1,1,0] 'FORWARD(REQUEST)']
Arrival order at network layer (out of order): [P2->P3 VC=[1,1,0] 'FORWARD(REQUEST)'], [P1->P3 VC=[1,0,0] 'REQUEST']
Buffer activity at P3:
    BUFFER  [P2->P3 VC=[1,1,0] 'FORWARD(REQUEST)']  (waiting for earlier message(s) from P1)
    DELIVER [P1->P3 VC=[1,0,0] 'REQUEST']
    DELIVER [P2->P3 VC=[1,1,0] 'FORWARD(REQUEST)']
Causal order respected everywhere  : True

===== TOTAL ORDERING SIMULATION =====
Total-order delivery at P1        : [P3->P1 G=1 'UPDATE_Z'], [P2->P1 G=2 'UPDATE_Y'], [P1->P1 G=3 'UPDATE_X']
Total-order delivery at P2        : [P3->P2 G=1 'UPDATE_Z'], [P2->P2 G=2 'UPDATE_Y'], [P1->P2 G=3 'UPDATE_X']
Total-order delivery at P3        : [P3->P3 G=1 'UPDATE_Z'], [P2->P3 G=2 'UPDATE_Y'], [P1->P3 G=3 'UPDATE_X']
Same global order at every process : True
```

## Test cases (report section 10)

| ID | Covered by |
|---|---|
| TC01 one sender, out of order | `TestFifo.test_tc01_single_sender_out_of_order` |
| TC02 two senders interleave | `TestFifo.test_tc02_two_senders_interleave` |
| TC03 dependent message arrives first | `TestCausal.test_tc03_*` |
| TC04 concurrent messages | `TestCausal.test_tc04_concurrent_messages_no_buffering` |
| TC05 same total order at every receiver | `TestTotal.test_tc05_same_order_everywhere` |
| TC06 future sequence number is buffered | `TestFifo.test_tc06_future_message_is_buffered` |
| TC07 sequencer assigns the global order | `TestTotal.test_tc07_*` |

Extra: a randomised stress test (`stress_causal`) runs 4 processes with random multicasts and random
network delivery, and `checks.py` independently verifies every delivery log.

## Project layout

```
message_ordering_simulator.py   CLI entry point
message_ordering/
    message.py     Message
    network.py     Network (reordering)
    process.py     Process (counters and clocks)
    ordering.py    FifoBuffer, CausalBuffer, Sequencer, TotalOrderBuffer
    checks.py      independent verifiers (is_fifo, is_causal, is_total)
    scenarios.py   the simulations + stress test
tests/             unit tests
```
