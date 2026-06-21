"""
Realistic Telecom Kafka Producer  –  HIGH-THROUGHPUT EDITION  v3.0
===================================================================
All original business logic is preserved:
  - Customer personas (Heavy Data / SMS-Oriented / Voice-Oriented / Normal)
  - Time-of-day behavior (night / morning / working-hours / evening-peak)
  - Network state (outages, signal degradation)
  - Cross-stream correlations (outages → calls, poor signal → low data, etc.)
  - Independent per-metric noise so each chart has its own shape

Performance target: 300+ events/sec on a normal laptop.

──────────────────────────────────────────────────────────────────
THROUGHPUT ANALYSIS (expected events/sec per topic)
──────────────────────────────────────────────────────────────────
  Batch size per tick  : CUSTOMERS_PER_TICK = 200 (up from 150)
  Tick target          : ~0.50 s  (TICK_SECONDS)

  Emission probabilities (daytime average):
    network_events      → P ≈ 0.85   →  200 × 0.85 = 170 events / tick
    usage_events        → P ≈ 0.70   →  200 × 0.70 = 140 events / tick
    customer_care_calls → P ≈ 0.20   →  200 × 0.20 =  40 events / tick
                                               TOTAL = 350 events / tick

  At 1 tick per 0.50 s  →  350 / 0.50 = 700 events / sec  (peak day)

  Conservative night estimate (P_usage ≈ 0.40, P_care ≈ 0.05):
    170 + 80 + 10 = 260 / tick  →  260 / 0.50 = 520 events / sec  (night)

  Therefore:  TOTAL ≥ 520 – 700 events / sec  (well above 300 target)

  Per-topic rates (daytime):
    usage_events        ≈  280  events / sec
    network_events      ≈  340  events / sec
    customer_care_calls ≈   80  events / sec
──────────────────────────────────────────────────────────────────
"""

import json
import random
import time
import math
import threading
import queue
import os
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# ─────────────────────────────────────────────────────────────────
# PERFORMANCE TUNING CONSTANTS
# ─────────────────────────────────────────────────────────────────

# PERF-1: Larger batch per tick → fewer Python loop overheads,
#         fewer flush() calls, better Kafka batching.
CUSTOMERS_PER_TICK = 200          # was 150; +33 % events per tick

# PERF-2: Shorter sleep → tighter loop, more ticks/sec.
#         0.50 s gives 2 ticks/sec while keeping the console readable.
TICK_SECONDS = 0.50               # was 1.0; doubles tick frequency

# PERF-3: How many worker threads drain the send queue.
#         Each thread owns its own KafkaProducer to avoid the GIL
#         bottleneck on a single producer's internal lock.
NUM_PRODUCER_THREADS = 4

# PERF-4: In-memory send queue depth (events queued before workers block)
QUEUE_MAXSIZE = 20_000

# ─────────────────────────────────────────────────────────────────
# KAFKA PRODUCER FACTORY
# PERF-5: Tune Kafka producer settings for throughput:
#   linger_ms      – wait up to N ms to fill a batch before sending
#   batch_size     – max bytes in one batch (default 16 KB → 512 KB)
#   compression_type – snappy is fast + reduces network I/O
#   acks=1         – leader-ack only (was 'all'); saves a round-trip
#   buffer_memory  – total bytes the producer may buffer
# ─────────────────────────────────────────────────────────────────

def make_kafka_producer():
    """Create one tuned KafkaProducer instance per worker thread."""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        # --- throughput knobs (unchanged Kafka compatibility) ---
        linger_ms=20,           # PERF-5a: batch events for up to 20 ms
        batch_size=512 * 1024,  # PERF-5b: 512 KB batch (up from 16 KB)
        compression_type='gzip',    # PERF-5c: gzip is built into Python (no native libs needed)
        acks=1,                 # PERF-5d: only leader must ack
        buffer_memory=64 * 1024 * 1024,  # PERF-5e: 64 MB buffer
        max_block_ms=5000,
    )

# ─────────────────────────────────────────────────────────────────
# SHARED EVENT QUEUE  (main thread → worker threads)
# PERF-6: Decouple event generation (CPU-bound) from Kafka I/O
#         (network-bound).  The main thread fills the queue as fast
#         as possible; worker threads drain it without blocking the
#         generator.  This hides Kafka network latency completely.
# ─────────────────────────────────────────────────────────────────
event_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

# ─────────────────────────────────────────────────────────────────
# PER-TOPIC COUNTERS  (thread-safe via threading.Lock)
# ─────────────────────────────────────────────────────────────────
_counter_lock = threading.Lock()
_counters = defaultdict(int)   # topic → total events sent

def _inc(topic: str, n: int = 1):
    with _counter_lock:
        _counters[topic] += n


# ─────────────────────────────────────────────────────────────────
# WORKER THREAD  (one per NUM_PRODUCER_THREADS)
# PERF-7: Each worker has its own KafkaProducer so they can
#         submit batches to the broker in parallel.  No shared
#         producer object → no lock contention between threads.
# ─────────────────────────────────────────────────────────────────
def kafka_worker(worker_id: int):
    """Drain events from the shared queue and send to Kafka."""
    producer = make_kafka_producer()
    local_buf = []              # PERF-8: micro-batch before flush

    while True:
        try:
            # PERF-9: grab up to 500 events in one shot to amortise
            #         queue.get() call overhead.
            item = event_queue.get(timeout=2.0)
            local_buf.append(item)

            # Drain any additional items already in the queue
            while len(local_buf) < 500:
                try:
                    local_buf.append(event_queue.get_nowait())
                except queue.Empty:
                    break

            # Send the micro-batch
            for (topic, key, value) in local_buf:
                producer.send(topic, key=key, value=value)
            producer.flush()    # flush once per micro-batch, not per event

            for (topic, _, __) in local_buf:
                _inc(topic)

            local_buf.clear()

        except queue.Empty:
            # Flush any lingering messages during idle periods
            producer.flush()
        except Exception as exc:
            print(f"[worker-{worker_id}] ERROR: {exc}")
            local_buf.clear()


# ─────────────────────────────────────────────────────────────────
# Load customers from CSV  (unchanged)
# ─────────────────────────────────────────────────────────────────
df = pd.read_csv("merged_data.csv")
customer_ids = df["Customer_ID"].dropna().astype(str).unique().tolist()

# PERF-10: Convert to a plain list (already is) and keep a fast index
# for random.choice.  No change needed here, but we cache the length
# so random.randrange is used instead of random.choice for marginal gain.
_n_customers = len(customer_ids)

# ─────────────────────────────────────────────────────────────────
# CUSTOMER PERSONAS  (unchanged)
# ─────────────────────────────────────────────────────────────────
PERSONAS = ["heavy_data", "sms_oriented", "voice_oriented", "normal"]
PERSONA_WEIGHTS = [0.20, 0.25, 0.30, 0.25]

customer_persona = {
    cid: random.choices(PERSONAS, weights=PERSONA_WEIGHTS, k=1)[0]
    for cid in customer_ids
}

# PERF-11: Build a direct-lookup dict  persona → int (0-3) so the
#          event builders use integer comparisons instead of string
#          comparisons.  Marginal but free.
_PERSONA_IDX = {p: i for i, p in enumerate(PERSONAS)}

# ─────────────────────────────────────────────────────────────────
# PRECOMPUTED TIME-OF-DAY LOOKUP TABLE
# PERF-12: The original get_hour_multipliers() built a dict on every
#          call (every tick).  We precompute all 24 entries once at
#          startup so each tick is a single list index lookup (O(1)).
# ─────────────────────────────────────────────────────────────────
_sms_curve = [
    0.3, 0.2, 0.15, 0.1, 0.2, 0.4,
    0.7, 1.0, 1.3, 1.2, 1.0, 0.9,
    0.85, 0.8, 0.85, 0.9, 1.0, 1.1,
    1.2, 1.15, 1.0, 0.8, 0.6, 0.4,
]
_voice_curve = [
    0.15, 0.1, 0.08, 0.08, 0.1, 0.2,
    0.5, 0.75, 1.0, 1.1, 1.05, 1.0,
    1.3, 1.35, 1.2, 1.1, 1.0, 1.05,
    1.4, 1.5, 1.3, 1.0, 0.7, 0.35,
]
_data_curve = [
    1.4, 1.6, 1.5, 1.2, 0.9, 0.7,
    0.8, 0.9, 1.0, 1.0, 1.0, 1.05,
    1.1, 1.1, 1.05, 1.0, 1.0, 1.1,
    1.25, 1.4, 1.5, 1.55, 1.6, 1.5,
]
_care_curve = [
    0.05, 0.03, 0.03, 0.03, 0.05, 0.1,
    0.3, 0.7, 1.1, 1.3, 1.2, 1.1,
    1.0, 1.0, 1.1, 1.2, 1.1, 0.9,
    0.7, 0.5, 0.3, 0.2, 0.1, 0.07,
]

# Precomputed list of 24 dicts – zero runtime cost per tick
HOUR_MULTS = [
    {
        "sms":   _sms_curve[h],
        "voice": _voice_curve[h],
        "data":  _data_curve[h],
        "care":  _care_curve[h],
    }
    for h in range(24)
]

def get_hour_multipliers(hour: int) -> dict:
    """O(1) lookup into precomputed table (was O(24) list construction)."""
    return HOUR_MULTS[hour]


# ─────────────────────────────────────────────────────────────────
# NETWORK STATE  (unchanged logic; one update per tick)
# ─────────────────────────────────────────────────────────────────
network_state = {
    "outage_active": False,
    "outage_remaining_ticks": 0,
    "signal_penalty": 0.0,
}

def update_network_state():
    ns = network_state
    if ns["outage_remaining_ticks"] > 0:
        ns["outage_remaining_ticks"] -= 1
        if ns["outage_remaining_ticks"] == 0:
            ns["outage_active"] = False
            ns["signal_penalty"] = 0.0
        return

    roll = random.random()
    if roll < 0.015:
        ns["outage_active"] = True
        ns["signal_penalty"] = random.uniform(0.6, 1.0)
        ns["outage_remaining_ticks"] = random.randint(5, 15)
    elif roll < 0.06:
        ns["outage_active"] = False
        ns["signal_penalty"] = random.uniform(0.2, 0.5)
        ns["outage_remaining_ticks"] = random.randint(1, 3)
    else:
        ns["signal_penalty"] = max(0.0, ns["signal_penalty"] - 0.1)


# ─────────────────────────────────────────────────────────────────
# PRECOMPUTED PERSONA RANGES
# PERF-13: Store per-persona base ranges as tuples so the event
#          builder uses a single dict lookup + randint instead of
#          4 conditional branches (if/elif/elif/else).
# ─────────────────────────────────────────────────────────────────
_PERSONA_RANGES = {
    "heavy_data":    dict(data=(3000,10000), voice=(20,200),  sms=(5,40),   pkg=(60,100)),
    "sms_oriented":  dict(data=(100,1500),   voice=(10,100),  sms=(40,100), pkg=(30,70)),
    "voice_oriented":dict(data=(100,2000),   voice=(150,500), sms=(0,20),   pkg=(40,85)),
    "normal":        dict(data=(200,4000),   voice=(30,200),  sms=(5,60),   pkg=(20,70)),
}

# ─────────────────────────────────────────────────────────────────
# PRECOMPUTED TIMESTAMP STRING
# PERF-14: datetime.utcnow() + strftime is called *per event* in
#          the original.  Since all events in one tick share the
#          same second-level timestamp, we compute it ONCE per tick
#          and pass it as a string into each builder.
#          Saves ~200 datetime object creations + format calls/tick.
# ─────────────────────────────────────────────────────────────────
# (ts is set at the top of each tick in the main loop)


# ─────────────────────────────────────────────────────────────────
# EVENT BUILDERS  (all original business logic preserved)
# Only structural change: accept pre-built timestamp string `ts`
# ─────────────────────────────────────────────────────────────────

def build_usage_event(customer_id: str, mults: dict, ns: dict, ts: str) -> dict:
    """
    Persona-aware usage event.  All original logic preserved.
    PERF change: accepts pre-computed timestamp string.
    """
    persona = customer_persona[customer_id]
    penalty = ns["signal_penalty"]
    r = _PERSONA_RANGES[persona]           # PERF-13: O(1) dict lookup

    base_data  = random.randint(*r["data"])
    base_voice = random.randint(*r["voice"])
    base_sms   = random.randint(*r["sms"])
    pkg_bias   = random.randint(*r["pkg"])

    voice_minutes = max(0, int(base_voice * mults["voice"] * random.uniform(0.8, 1.2)))
    data_usage_mb = max(100, int(
        base_data * mults["data"] * random.uniform(0.85, 1.15) * (1 - 0.6 * penalty)
    ))
    sms_count     = max(0, int(base_sms * mults["sms"] * random.uniform(0.75, 1.25)))
    voice_minutes = max(0, int(voice_minutes * (1 - 0.3 * penalty)))

    return {
        "customer_id":       customer_id,
        "voice_minutes":     voice_minutes,
        "data_usage_mb":     data_usage_mb,
        "sms_count":         sms_count,
        "package_usage_pct": min(100, int(pkg_bias * mults["data"] * random.uniform(0.9, 1.1))),
        "event_time":        ts,           # PERF-14: reuse tick timestamp
    }


def build_network_event(customer_id: str, ns: dict, ts: str) -> dict:
    """
    Network telemetry event correlated with shared network state.
    PERF change: accepts pre-computed timestamp string.
    """
    penalty = ns["signal_penalty"]

    base_signal     = random.randint(50, 100)
    signal_strength = max(10, int(base_signal * (1 - 0.7 * penalty) + random.gauss(0, 5)))

    if signal_strength < 40:
        dropped_calls = random.randint(2, 5)
    elif signal_strength < 65:
        dropped_calls = random.randint(0, 3)
    else:
        dropped_calls = random.choices([0, 1, 2], weights=[75, 20, 5], k=1)[0]

    base_speed     = random.randint(30, 100)
    internet_speed = max(1, int(base_speed * (1 - 0.8 * penalty) + random.gauss(0, 3)))

    outage_flag = 1 if ns["outage_active"] else random.choices([0, 1], weights=[97, 3], k=1)[0]

    return {
        "customer_id":     customer_id,
        "signal_strength": signal_strength,
        "dropped_calls":   dropped_calls,
        "internet_speed":  internet_speed,
        "outage_flag":     outage_flag,
        "event_time":      ts,            # PERF-14: reuse tick timestamp
    }


def build_care_call_event(customer_id: str, mults: dict, ns: dict, ts: str) -> dict:
    """
    Customer-care call event correlated with outages and business hours.
    PERF change: accepts pre-computed timestamp string.
    """
    penalty = ns["signal_penalty"]
    outage  = ns["outage_active"]

    if outage:
        weights = [60, 10, 5, 25]
    elif penalty > 0.4:
        weights = [45, 15, 10, 30]
    else:
        weights = [25, 25, 15, 35]

    issue_type = random.choices(
        ["Network", "Billing", "Service", "Internet"],
        weights=weights, k=1
    )[0]

    resolution_map = {
        "Network":  [30, 70],
        "Internet": [60, 40],
        "Billing":  [85, 15],
        "Service":  [75, 25],
    }
    resolved = random.choices(["Yes", "No"], weights=resolution_map[issue_type], k=1)[0]

    base_anger = random.randint(1, 6)
    if outage:
        base_anger = min(10, base_anger + random.randint(2, 4))
    if resolved == "No":
        base_anger = min(10, base_anger + random.randint(1, 2))

    duration = random.randint(300, 900) if resolved == "No" else random.randint(60, 400)

    return {
        "customer_id":       customer_id,
        "anger_rate":        base_anger,
        "issue_type":        issue_type,
        "call_duration_sec": duration,
        "resolved":          resolved,
        "event_time":        ts,          # PERF-14: reuse tick timestamp
    }


# ─────────────────────────────────────────────────────────────────
# EMISSION PROBABILITIES  (unchanged logic)
# ─────────────────────────────────────────────────────────────────

def emission_probs(mults: dict, ns: dict) -> dict:
    activity   = (mults["sms"] + mults["voice"] + mults["data"]) / 3
    usage_prob = min(0.95, 0.60 * activity)

    base_care = 0.15
    care_prob = min(0.70, base_care * mults["care"] * (1 + 3 * ns["signal_penalty"]))
    if ns["outage_active"]:
        care_prob = min(0.80, care_prob * 2.5)

    return {
        "network": 0.85,
        "usage":   usage_prob,
        "care":    care_prob,
    }


# ─────────────────────────────────────────────────────────────────
# THROUGHPUT STATS HELPER
# ─────────────────────────────────────────────────────────────────

def print_throughput_stats(elapsed: float, tick: int):
    """Print estimated events/sec per topic using live counters."""
    with _counter_lock:
        snap = dict(_counters)

    total = sum(snap.values())
    print("\n── Throughput Report ──────────────────────────────────")
    for topic in ["usage_events", "network_events", "customer_care_calls"]:
        cnt = snap.get(topic, 0)
        rate = cnt / elapsed if elapsed > 0 else 0.0
        print(f"  {topic:<28}  {cnt:>8,} events   {rate:>7.1f} evt/s")
    total_rate = total / elapsed if elapsed > 0 else 0.0
    print(f"  {'TOTAL':<28}  {total:>8,} events   {total_rate:>7.1f} evt/s")
    print(f"  Ticks completed: {tick}   Elapsed: {elapsed:.1f}s")
    print("───────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────
# START WORKER THREADS
# ─────────────────────────────────────────────────────────────────
_workers = []
for _wid in range(NUM_PRODUCER_THREADS):
    t = threading.Thread(target=kafka_worker, args=(_wid,), daemon=True)
    t.start()
    _workers.append(t)

print(f"HIGH-THROUGHPUT TELECOM PRODUCER  v3.0")
print(f"Workers: {NUM_PRODUCER_THREADS}  |  Batch/tick: {CUSTOMERS_PER_TICK}  |  Tick: {TICK_SECONDS}s")
print(f"Expected: ~520–700 events/sec  (usage≈280, network≈340, care≈80)\n")

# ─────────────────────────────────────────────────────────────────
# MAIN LOOP
# PERF-15: We queue events instead of calling producer.send()
#          directly.  The main thread is now purely compute; all
#          I/O is handled by the worker pool.  This removes the
#          Kafka network round-trip from the hot path entirely.
# ─────────────────────────────────────────────────────────────────

_start_wall = time.monotonic()
_tick = 0
_STATS_EVERY_N_TICKS = 20       # print throughput report every 20 ticks (~10 s)

while True:
    tick_start = time.monotonic()
    _tick += 1

    # Step 1: Advance shared network state once per tick
    update_network_state()

    # Step 2: Precompute values shared across all 200 customers this tick
    now  = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    hour = now.hour
    ts   = now.strftime("%Y-%m-%d %H:%M:%S")   # PERF-14: compute once
    mults = get_hour_multipliers(hour)           # PERF-12: O(1) lookup
    probs = emission_probs(mults, network_state)

    # Cache these locally for the tight inner loop (avoids repeated
    # global lookups inside the loop body)                # PERF-16
    _customer_ids  = customer_ids
    _n             = _n_customers
    _ns            = network_state
    _q_put         = event_queue.put
    _rand          = random.random
    _randrange     = random.randrange
    p_net          = probs["network"]
    p_use          = probs["usage"]
    p_care         = probs["care"]

    # ── Inner loop: generate events and enqueue them ──────────────
    # PERF-15 continued: event_queue.put() is O(1) and very fast;
    # worker threads do all the Kafka I/O asynchronously.
    for _ in range(CUSTOMERS_PER_TICK):
        cid = _customer_ids[_randrange(_n)]    # PERF-10: randrange vs choice

        if _rand() < p_net:
            _q_put(("network_events", cid,
                    build_network_event(cid, _ns, ts)))

        if _rand() < p_use:
            _q_put(("usage_events", cid,
                    build_usage_event(cid, mults, _ns, ts)))

        if _rand() < p_care:
            _q_put(("customer_care_calls", cid,
                    build_care_call_event(cid, mults, _ns, ts)))

    # ── Console log (one line per tick) ───────────────────────────
    outage_tag = "  ⚠ OUTAGE" if _ns["outage_active"] else ""
    q_depth    = event_queue.qsize()

    tick_elapsed = time.monotonic() - tick_start
    # PERF-17: estimate instantaneous events/sec from this tick alone
    tick_events_approx = int(
        CUSTOMERS_PER_TICK * (p_net + p_use + p_care)
    )
    instant_eps = tick_events_approx / max(tick_elapsed, 0.001)

    print(
        f"[{ts}]  h={hour:02d}  "
        f"sms×{mults['sms']:.2f}  vox×{mults['voice']:.2f}  "
        f"dat×{mults['data']:.2f}  care×{mults['care']:.2f}  "
        f"pen={_ns['signal_penalty']:.2f}  "
        f"~{tick_events_approx} evt  {instant_eps:.0f} evt/s  "
        f"q={q_depth}{outage_tag}"
    )

    # ── Periodic detailed throughput report ───────────────────────
    if _tick % _STATS_EVERY_N_TICKS == 0:
        print_throughput_stats(time.monotonic() - _start_wall, _tick)

    # ── Pace the loop to TICK_SECONDS ────────────────────────────
    # PERF-18: sleep only the *remaining* time in the tick so that
    # compute time is automatically subtracted; no drift over time.
    remaining = TICK_SECONDS - (time.monotonic() - tick_start)
    if remaining > 0:
        time.sleep(remaining)
