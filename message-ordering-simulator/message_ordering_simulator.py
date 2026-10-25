#!/usr/bin/env python3
"""
MESSAGE-ORDERING SIMULATOR
Design and Implementation of FIFO, Causal, and Total Ordering of Messages

Usage:
    python message_ordering_simulator.py                 # run all three simulations
    python message_ordering_simulator.py causal --seed 3  # one simulation
    python message_ordering_simulator.py --stress         # randomised causal-order stress test
"""
import argparse
import sys

from message_ordering import (simulate_fifo_ordering, simulate_causal_ordering,
                              simulate_concurrent_causal, simulate_total_ordering,
                              stress_causal)


def main() -> int:
    ap = argparse.ArgumentParser(description="Message-Ordering Simulator (FIFO / Causal / Total)")
    ap.add_argument("mode", nargs="?", default="all", choices=["all", "fifo", "causal", "total"])
    ap.add_argument("--seed", type=int, default=None, help="random seed for the network layer")
    ap.add_argument("--stress", action="store_true", help="run the randomised causal stress test")
    args = ap.parse_args()

    print("---------- MESSAGE ORDERING SIMULATOR ----------")
    results = []
    kw = {} if args.seed is None else {"seed": args.seed}
    if args.mode in ("all", "fifo"):
        results.append(simulate_fifo_ordering(**kw))
    if args.mode in ("all", "causal"):
        results.append(simulate_causal_ordering(**kw))
        results.append(simulate_concurrent_causal(**kw))
    if args.mode in ("all", "total"):
        results.append(simulate_total_ordering(**kw))

    if args.stress:
        print("\n===== STRESS TEST: causal order, 4 processes, 30 multicasts, 50 seeds =====")
        bad = [s for s in range(50) if not stress_causal(4, 30, s).ok]
        print("All runs causally ordered and fully delivered:", not bad)
        if bad:
            print("Failing seeds:", bad)
            return 1

    print("\n---------- SIMULATION COMPLETE ----------")
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
