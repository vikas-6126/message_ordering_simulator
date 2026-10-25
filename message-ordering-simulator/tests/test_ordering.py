import unittest
from dataclasses import replace

from message_ordering import (CausalBuffer, FifoBuffer, Message, Network, Process, Sequencer,
                              TotalOrderBuffer, simulate_causal_ordering,
                              simulate_concurrent_causal, simulate_fifo_ordering,
                              simulate_total_ordering, stress_causal)
from message_ordering.checks import is_causal, is_fifo


class TestFifo(unittest.TestCase):
    def test_tc01_single_sender_out_of_order(self):
        p = Process("P1", ["P1", "P2", "P3"])
        msgs = [p.make_message_fifo("P3", f"A{i}") for i in range(1, 6)]
        buf, out = FifoBuffer(), []
        for m in reversed(msgs):                    # worst case: fully reversed
            out.extend(buf.receive(m))
        self.assertEqual([m.content for m in out], ["A1", "A2", "A3", "A4", "A5"])
        self.assertEqual(buf.pending, 0)

    def test_tc02_two_senders_interleave(self):
        for seed in range(20):
            r = simulate_fifo_ordering(seed=seed, verbose=False)
            self.assertTrue(r.ok)
            self.assertEqual(len(r.delivered["P3"]), 5)
            self.assertTrue(is_fifo(r.delivered["P3"]))

    def test_tc06_future_message_is_buffered(self):
        p = Process("P1", ["P1", "P2", "P3"])
        m1, m2 = p.make_message_fifo("P3", "x"), p.make_message_fifo("P3", "y")
        buf = FifoBuffer()
        self.assertEqual(buf.receive(m2), [])       # arrives early -> held back
        self.assertEqual(buf.pending, 1)
        self.assertEqual(buf.receive(m1), [m1, m2])  # predecessor releases both

    def test_duplicate_is_ignored(self):
        p = Process("P1", ["P1", "P2", "P3"])
        m1 = p.make_message_fifo("P3", "x")
        buf = FifoBuffer()
        self.assertEqual(buf.receive(m1), [m1])
        self.assertEqual(buf.receive(replace(m1)), [])


class TestCausal(unittest.TestCase):
    def test_tc03_dependent_message_withheld(self):
        r = simulate_causal_ordering(verbose=False)
        self.assertEqual([m.content for m in r.arrival["P3"]], ["FORWARD(REQUEST)", "REQUEST"])
        self.assertEqual([m.content for m in r.delivered["P3"]], ["REQUEST", "FORWARD(REQUEST)"])
        self.assertTrue(r.ok)

    def test_tc03_m2_alone_is_buffered(self):
        pids = ["P1", "P2", "P3"]
        p1, p2, p3 = (Process(p, pids) for p in pids)
        m1 = p1.make_message_causal("P2", "m1")
        p2.causal.receive(m1)
        m2 = p2.make_message_causal("P3", "m2")
        self.assertEqual(p3.causal.receive(m2), [])
        self.assertEqual(len(p3.causal.pending), 1)
        m1_for_p3 = replace(m1, receiver="P3")
        self.assertEqual([m.content for m in p3.causal.receive(m1_for_p3)], ["m1", "m2"])

    def test_tc04_concurrent_messages_no_buffering(self):
        for seed in range(10):
            r = simulate_concurrent_causal(seed=seed, verbose=False)
            self.assertTrue(r.ok)
            self.assertEqual(r.pending["P3"], 0)

    def test_stress_random_executions(self):
        for seed in range(30):
            r = stress_causal(num_processes=4, num_messages=25, seed=seed)
            self.assertTrue(r.ok, f"seed {seed}")

    def test_checker_detects_violation(self):
        pids = ["P1", "P2"]
        p1 = Process("P1", pids)
        a, b = p1.make_message_causal("P2", "a"), p1.make_message_causal("P2", "b")
        self.assertTrue(is_causal({"P2": [a, b]}))
        self.assertFalse(is_causal({"P2": [b, a]}))


class TestTotal(unittest.TestCase):
    def test_tc05_same_order_everywhere(self):
        for seed in range(20):
            r = simulate_total_ordering(seed=seed, verbose=False)
            self.assertTrue(r.ok)
            orders = {tuple(m.content for m in d) for d in r.delivered.values()}
            self.assertEqual(len(orders), 1)

    def test_tc07_sequencer_assigns_unique_consecutive_numbers(self):
        seq = Sequencer()
        base = Message("P1", "ALL", "x")
        self.assertEqual([seq.stamp(base).global_seq for _ in range(5)], [1, 2, 3, 4, 5])

    def test_total_buffer_holds_gaps(self):
        seq, buf = Sequencer(), TotalOrderBuffer()
        ms = [seq.stamp(Message("P1", "P2", c)) for c in "abc"]
        self.assertEqual(buf.receive(ms[2]), [])
        self.assertEqual(buf.receive(ms[0]), [ms[0]])
        self.assertEqual(buf.receive(ms[1]), [ms[1], ms[2]])


class TestNetwork(unittest.TestCase):
    def test_flush_actually_reorders(self):
        net = Network(seed=1)
        for i in range(5):
            net.send(Message("P1", "P3", str(i)))
        out = [m.content for m in net.flush_to("P3")]
        self.assertNotEqual(out, ["0", "1", "2", "3", "4"])
        self.assertEqual(sorted(out), ["0", "1", "2", "3", "4"])


if __name__ == "__main__":
    unittest.main()
