# nostr-measure

Measurement tool for a CS 440 (Northwestern, Fall 2026) reproducibility project: re-measuring
Wei & Tyson, *An Empirical Analysis of the Nostr Social Network: Decentralization, Availability,
and Replication Overhead* (ACM CoNEXT '25, https://arxiv.org/abs/2402.05709) on the 2026 network.

Kotlin/JVM, OkHttp WebSockets, SQLite. No framework, no ORM. Every measurement is something an
ordinary Nostr client does anyway: open a socket, send one `REQ`, read the NIP-11 document.

## Build and run

```
./gradlew installDist
export NOSTR_MEASURE_CONTACT="you@example.edu"     # goes into the User-Agent so relay operators can reach you
bin=build/install/nostr-measure/bin/nostr-measure

$bin discover                      # NIP-66 relay discovery (kind 30166 from monitor relays) + NIP-11 for every relay
$bin probe                         # one availability tick over every known relay (~20 s for 1,200 relays)
$bin probe --interval=15           # the same, forever, aligned to :00/:15/:30/:45
$bin geo                           # DNS + ip-api.com batch lookups -> AS, country, CDN flag
$bin sample --n=300 --set=poc300   # draw recent kind-1 ids from four large relays
$bin replicate --set=poc300        # ask every relay that answered the last tick which sampled ids it holds
$bin relay-lists --set=poc300      # kind-10002 (NIP-65) lists for the sampled authors
$bin rate --relay=wss://relay.damus.io --minutes=5   # passive live event rate on one relay
$bin report --set=poc300           # Markdown tables -> stdout and out/report-*.md
```

Data lives in `data/nostr-measure.db` (override with `NOSTR_MEASURE_DB`). Every job is idempotent
and resumable: `replicate` keeps a per-relay cursor, `probe` writes one transaction per tick.

## What each command measures (paper figure it reproduces)

| command | measurement | paper |
|---|---|---|
| `discover` | relay population, software, free vs paid, advertised limits | §3.1, Fig. 2 context |
| `probe` | per-relay reachability every 15 min, connect and EOSE latency, failure class | Fig. 4a (uptime), dead relays |
| `geo` | relays by country and AS, CDN-fronted share | Fig. 2–3 |
| `sample` + `replicate` | relays (and ASes) holding each sampled post | Fig. 6 (replicas per post) |
| `relay-lists` | NIP-65 adoption among sampled authors; replication split by adoption | extends §6.1 |
| `report` takedown table | posts still available after removing the top-X relays | Fig. 7a |
| `rate` | observed event arrival rate on a relay (load, not capacity) | extension |

## Polite-client rules (enforced in code)

One socket per relay, one subscription in flight, batches never exceed the relay's advertised
`max_limit`, back off on `NOTICE`/`CLOSED`, skip relays that demand auth, and never load-test a
relay you do not own. Public data only; reporting is aggregate.

## Tests

`./gradlew test` — unit tests for URL canonicalization, event id/signature verification against
real events, NIP-11 and kind parsing, plus integration tests against a scripted fake relay on
OkHttp's MockWebServer (probe classification, ids batching, resume, foreign-id detection).
