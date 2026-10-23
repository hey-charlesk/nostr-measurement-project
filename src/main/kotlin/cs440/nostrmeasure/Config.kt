package cs440.nostrmeasure

object Config {
    private fun env(name: String): String? = System.getenv(name)?.takeIf { it.isNotBlank() }

    val dbPath: String = env("NOSTR_MEASURE_DB") ?: "data/nostr-measure.db"
    val rawDir: String = env("NOSTR_MEASURE_RAW") ?: "data/raw"
    val outDir: String = env("NOSTR_MEASURE_OUT") ?: "out"
    val contact: String = env("NOSTR_MEASURE_CONTACT") ?: "CS440 student, Northwestern"
    val userAgent: String = "nostr-measure/0.1 (academic measurement; $contact)"

    // NIP-66 monitors that publish kind-30166 relay discovery events.
    val nip66Monitors: List<String> = listOf(
        "wss://relay.nostr.watch",
        "wss://monitorlizard.nostr1.com",
    )

    // Fallback seed if every monitor is unreachable.
    val seedRelays: List<String> = listOf(
        "wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net", "wss://relay.nostr.band",
        "wss://purplepag.es", "wss://relay.snort.social", "wss://nostr.wine", "wss://nostr.mom",
        "wss://relay.nostr.bg", "wss://offchain.pub", "wss://nostr.oxtr.dev", "wss://relay.nostr.watch",
    )

    // Large, read-open relays used to draw the replication sample and to fetch kind-10002 lists.
    val sampleSourceRelays: List<String> = listOf(
        "wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net", "wss://nostr.wine",
    )
    val relayListSourceRelays: List<String> = listOf(
        "wss://purplepag.es", "wss://relay.damus.io", "wss://nos.lol",
    )

    const val probeConcurrency = 200
    const val probeConnectTimeoutMs = 10_000L
    const val probeEoseTimeoutMs = 10_000L
    const val probeTickDeadlineMs = 13 * 60_000L
    const val nip11Concurrency = 50
    const val nip11TimeoutMs = 5_000L
    const val replicateConcurrency = 50
    const val replicateEoseTimeoutMs = 20_000L
    const val replicateInterBatchMs = 200L
    const val replicateDefaultBatch = 100
}
