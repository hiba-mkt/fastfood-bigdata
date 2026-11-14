#!/usr/bin/env python3
"""Générateur de commandes FastFood.ma.

Exemples (dans le conteneur jupyter) :
  python /work/producer.py --mode file  --n 50000
  python /work/producer.py --mode kafka --rate 20
"""
import argparse
import heapq
import itertools
import json
import os
import random
import time
from datetime import datetime, timezone

CITIES = {
    "Casablanca": ("CAS", 33.5731, -7.5898),
    "Rabat": ("RAB", 34.0209, -6.8416),
    "Marrakech": ("MAR", 31.6295, -7.9811),
    "Tanger": ("TNG", 35.7595, -5.8340),
}
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger", "poulet", "healthy", "dessert"]
LATE_RATE, DUP_RATE, CANCEL_RATE = 0.05, 0.02, 0.08
MAX_LATE_SECONDS = 15 * 60


def iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_restaurants(path, rng):
    """200 restaurants, un objet JSON par ligne (JSON Lines)."""
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, 201):
            city = rng.choice(list(CITIES))
            _, lat, lon = CITIES[city]
            cat = rng.choice(CATEGORIES)
            row = {
                "restaurant_id": f"R-{i:03d}",
                "name": f"{cat.capitalize()} {city} {i}",
                "city": city,
                "category": cat,
                "latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
                "longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_restaurants(path, rng):
    if not os.path.exists(path):
        make_restaurants(path, rng)
        print(f"restaurants.json généré : {path}")
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def new_order(rng, restaurants, counter):
    r = rng.choice(restaurants)
    code = CITIES[r["city"]][0]
    items = rng.randint(1, 6)
    return {
        "order_id": f"ORD-{counter:07d}",
        "restaurant_id": r["restaurant_id"],
        "customer_id": f"C-{rng.randint(1, 20000):05d}",
        "city": r["city"],
        "amount_mad": round(items * rng.uniform(25, 70), 2),
        "items_count": items,
        "distance_km": round(rng.uniform(0.5, 9.0), 1),
        "courier_id": f"L-{code}-{rng.randint(1, 20):02d}",
    }


def lifecycle(rng, scale):
    """Liste (décalage en secondes, statut). scale < 1 accélère le temps (mode kafka)."""
    steps = [(0.0, "CREATED")]
    if rng.random() < CANCEL_RATE:
        steps.append((rng.uniform(60, 300) * scale, "CANCELLED"))
    else:
        t = rng.uniform(300, 900) * scale
        steps.append((t, "PICKED_UP"))
        steps.append((t + rng.uniform(600, 1800) * scale, "DELIVERED"))
    return steps


def make_events(rng, order, status, emit_ts):
    """Retourne [(emit_ts, événement)] avec éventuel retard et doublon."""
    event_ts = emit_ts
    if rng.random() < LATE_RATE:  # livreur hors réseau : event_time antérieur à l'émission
        event_ts = emit_ts - rng.uniform(60, MAX_LATE_SECONDS)
    ev = dict(order, status=status, event_time=iso(event_ts))
    out = [(emit_ts, ev)]
    if rng.random() < DUP_RATE:  # même order_id, même status
        out.append((emit_ts + rng.uniform(0, 2), dict(ev)))
    return out


def run_file(args, restaurants, rng):
    end = time.time() - 3600
    start = end - 7 * 86400
    events, counter = [], 0
    while len(events) < args.n:
        counter += 1
        order = new_order(rng, restaurants, counter)
        t0 = rng.uniform(start, end)
        for offset, status in lifecycle(rng, 1.0):
            events.extend(make_events(rng, order, status, t0 + offset))
    events.sort(key=lambda x: x[0])
    events = events[: args.n]
    with open(args.out, "w", encoding="utf-8") as f:
        for _, ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"{len(events)} événements écrits dans {args.out}")


def run_kafka(args, restaurants, rng):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    heap, seq = [], itertools.count()
    counter, carry, sent, last_log = 0, 0.0, 0, time.time()
    orders_per_sec = args.rate / 3.6  # ~3,6 événements par commande en moyenne
    tick = 0.1
    print(f"Envoi vers {args.bootstrap}, topic '{args.topic}', {args.rate} évén./s (Ctrl+C pour arrêter)")
    try:
        while True:
            now = time.time()
            carry += orders_per_sec * tick
            while carry >= 1:
                carry -= 1
                counter += 1
                order = new_order(rng, restaurants, counter)
                for offset, status in lifecycle(rng, 1 / 30):  # minutes -> secondes
                    for emit_ts, ev in make_events(rng, order, status, now + offset):
                        heapq.heappush(heap, (emit_ts, next(seq), ev))
            while heap and heap[0][0] <= now:
                _, _, ev = heapq.heappop(heap)
                producer.send(args.topic, key=ev["order_id"], value=ev)
                sent += 1
            if now - last_log >= 5:
                print(f"{sent} événements envoyés")
                last_log = now
            time.sleep(tick)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Arrêt. Total envoyé : {sent}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["file", "kafka"], required=True)
    p.add_argument("--n", type=int, default=50000, help="nombre d'événements (mode file)")
    p.add_argument("--rate", type=float, default=20, help="événements par seconde (mode kafka)")
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default="orders")
    p.add_argument("--out", default="/work/orders_history.jsonl")
    p.add_argument("--restaurants", default="/work/restaurants.json")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    rng = random.Random(args.seed)
    restaurants = load_restaurants(args.restaurants, rng)
    if args.mode == "file":
        run_file(args, restaurants, rng)
    else:
        run_kafka(args, restaurants, random.Random())


if __name__ == "__main__":
    main()