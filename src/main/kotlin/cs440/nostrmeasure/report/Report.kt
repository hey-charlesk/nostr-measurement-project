package cs440.nostrmeasure.report

import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.mapRows
import cs440.nostrmeasure.db.queryLong
import cs440.nostrmeasure.geo.Geo
import cs440.nostrmeasure.probe.percentile
import java.sql.Connection
import java.time.Instant

class Report(private val db: Db) {

    fun full(setName: String?): String = buildString {
        appendLine("# nostr-measure report — ${Instant.now()}")
        appendLine()
        append(inventory())
        append(latestTick())
        append(uptime())
        append(geo())
        val set = setName ?: db.read { latestSet(it) }
        if (set != null) {
            append(replication(set))
            append(takedown(set))
        } else {
            appendLine("## Replication\n\n_No sample set yet. Run `sample` and `replicate`._\n")
        }
    }

    fun inventory(): String = db.read { c ->
        val relays = c.queryLong("SELECT COUNT(*) FROM relays")
        val withNip11 = c.queryLong("SELECT COUNT(*) FROM latest_snapshot WHERE ok = 1")
        val paid = c.queryLong("SELECT COUNT(*) FROM latest_snapshot WHERE ok = 1 AND payment_required = 1")
        val free = c.queryLong("SELECT COUNT(*) FROM latest_snapshot WHERE ok = 1 AND (payment_required = 0 OR payment_required IS NULL)")
        val auth = c.queryLong("SELECT COUNT(*) FROM latest_snapshot WHERE ok = 1 AND auth_required = 1")
        val cdn = c.queryLong("SELECT COUNT(*) FROM latest_snapshot WHERE cf_ray = 1 OR lower(server_header) LIKE 'cloudflare%'")
        val software = c.createStatement().use { st ->
            st.executeQuery("SELECT COALESCE(software_name, '(not reported)') s, COUNT(*) n FROM latest_snapshot WHERE ok = 1 GROUP BY s ORDER BY n DESC LIMIT 12")
                .mapRows { Pair(it.getString(1), it.getLong(2)) }
        }
        buildString {
            appendLine("## Relay inventory")
            appendLine()
            appendLine("| metric | value |\n|---|---|")
            appendLine("| relays known | $relays |")
            appendLine("| with a NIP-11 document | $withNip11 |")
            appendLine("| paid (payment_required) | $paid |")
            appendLine("| free / unspecified | $free |")
            appendLine("| auth_required | $auth |")
            appendLine("| Cloudflare-fronted (header) | $cdn |")
            appendLine()
            appendLine("Software (self-reported, latest NIP-11):")
            appendLine()
            appendLine("| software | relays |\n|---|---|")
            software.forEach { (s, n) -> appendLine("| $s | $n |") }
            appendLine()
        }
    }

    fun latestTick(): String = db.read { c ->
        val ts = c.queryLong("SELECT MAX(ts) FROM probe_ticks")
        if (ts == 0L) return@read "## Latest probe tick\n\n_No probes yet._\n\n"
        val total = c.queryLong("SELECT relays_total FROM probe_ticks WHERE ts = $ts")
        val probed = c.queryLong("SELECT probed FROM probe_ticks WHERE ts = $ts")
        val dur = c.queryLong("SELECT duration_ms FROM probe_ticks WHERE ts = $ts")
        val byClass = c.createStatement().use { st ->
            st.executeQuery("SELECT error_class, COUNT(*) FROM probes WHERE ts = $ts GROUP BY error_class ORDER BY 2 DESC").mapRows { Pair(it.getString(1), it.getLong(2)) }
        }
        val connect = c.createStatement().use { st -> st.executeQuery("SELECT connect_ms FROM probes WHERE ts = $ts AND ws_ok = 1").mapRows { it.getLong(1) } }
        val eose = c.createStatement().use { st -> st.executeQuery("SELECT eose_ms FROM probes WHERE ts = $ts AND eose_ok = 1").mapRows { it.getLong(1) } }
        buildString {
            appendLine("## Latest probe tick (${Instant.ofEpochSecond(ts)})")
            appendLine()
            appendLine("| metric | value |\n|---|---|")
            appendLine("| relays probed | $probed / $total |")
            appendLine("| socket opened (ws_ok) | ${connect.size} (${pct(connect.size, probed)}) |")
            appendLine("| answered a REQ (eose_ok) | ${eose.size} (${pct(eose.size, probed)}) |")
            appendLine("| connect ms p50 / p90 / p95 | ${percentile(connect, 50)} / ${percentile(connect, 90)} / ${percentile(connect, 95)} |")
            appendLine("| EOSE ms p50 / p90 / p95 | ${percentile(eose, 50)} / ${percentile(eose, 90)} / ${percentile(eose, 95)} |")
            appendLine("| tick duration | ${dur / 1000} s |")
            appendLine()
            appendLine("| outcome | relays |\n|---|---|")
            byClass.forEach { (k, n) -> appendLine("| $k | $n |") }
            appendLine()
        }
    }

    fun uptime(): String = db.read { c ->
        val ticks = c.queryLong("SELECT COUNT(*) FROM probe_ticks")
        if (ticks == 0L) return@read ""
        val first = c.queryLong("SELECT MIN(ts) FROM probe_ticks")
        val last = c.queryLong("SELECT MAX(ts) FROM probe_ticks")
        val perRelay = c.createStatement().use { st ->
            st.executeQuery("SELECT relay_id, COUNT(*) n, SUM(ws_ok) up FROM probes GROUP BY relay_id").mapRows { Triple(it.getLong(1), it.getLong(2), it.getLong(3)) }
        }
        val buckets = linkedMapOf("100%" to 0, "99–100%" to 0, "90–99%" to 0, "50–90%" to 0, "1–50%" to 0, "0% (never up)" to 0)
        for ((_, n, up) in perRelay) {
            val u = up.toDouble() / n
            val key = when {
                up == n -> "100%"; u >= 0.99 -> "99–100%"; u >= 0.90 -> "90–99%"; u >= 0.50 -> "50–90%"; up > 0 -> "1–50%"; else -> "0% (never up)"
            }
            buckets[key] = buckets.getValue(key) + 1
        }
        val everUp = perRelay.count { it.third > 0 }
        buildString {
            appendLine("## Uptime so far")
            appendLine()
            appendLine("$ticks ticks from ${Instant.ofEpochSecond(first)} to ${Instant.ofEpochSecond(last)}; ${perRelay.size} relays probed, $everUp ever reachable.")
            appendLine()
            appendLine("| uptime bucket | relays | share of ever-reachable |\n|---|---|---|")
            buckets.forEach { (k, n) -> appendLine("| $k | $n | ${if (k.startsWith("0%")) "" else pct(n, everUp)} |") }
            appendLine()
        }
    }

    fun geo(): String = db.read { c ->
        val mapped = c.queryLong("SELECT COUNT(DISTINCT l.relay_id) FROM latest_relay_ip l JOIN ip_info i ON i.ip = l.ip WHERE i.asn IS NOT NULL")
        if (mapped == 0L) return@read "## Hosting\n\n_No geo data yet. Run `geo`._\n\n"
        val reachable = c.queryLong("SELECT COUNT(*) FROM latest_probe WHERE ws_ok = 1")
        // One row per relay: its first mapped IP decides country/AS (multi-IP relays are rare).
        val base = "SELECT l.relay_id, MIN(i.country) country, MIN(i.asn) asn, MIN(i.as_name) as_name FROM latest_relay_ip l JOIN ip_info i ON i.ip = l.ip " +
            "JOIN latest_probe p ON p.relay_id = l.relay_id AND p.ws_ok = 1 WHERE i.asn IS NOT NULL GROUP BY l.relay_id"
        val rows = c.createStatement().use { st -> st.executeQuery(base).mapRows { Triple(it.getString("country") ?: "??", it.getInt("asn"), it.getString("as_name") ?: "") } }
        val byCountry = rows.groupingBy { it.first }.eachCount().entries.sortedByDescending { it.value }
        val byAs = rows.groupingBy { Pair(it.second, it.third) }.eachCount().entries.sortedByDescending { it.value }
        val cdn = rows.count { it.second in Geo.CDN_ASNS }
        buildString {
            appendLine("## Hosting (reachable relays with an AS mapping: ${rows.size} of $reachable)")
            appendLine()
            appendLine("${byCountry.size} countries, ${byAs.size} ASes; $cdn relays sit behind a CDN AS (origin hidden).")
            appendLine()
            appendLine("| country | relays | share |\n|---|---|---|")
            byCountry.take(15).forEach { appendLine("| ${it.key} | ${it.value} | ${pct(it.value, rows.size)} |") }
            appendLine()
            appendLine("| AS | name | relays | share |\n|---|---|---|---|")
            byAs.take(15).forEach { appendLine("| AS${it.key.first} | ${it.key.second.take(32)} | ${it.value} | ${pct(it.value, rows.size)} |") }
            appendLine()
        }
    }

    data class PostReplicas(val eventId: String, val pubkey: String, val relays: Set<Long>)

    private fun loadReplicas(c: Connection, set: String): List<PostReplicas> {
        val posts = c.prepareStatement("SELECT s.event_id, e.pubkey FROM samples s JOIN events e ON e.id = s.event_id WHERE s.set_name = ?").use { ps ->
            ps.setString(1, set); ps.executeQuery().mapRows { Pair(it.getString(1), it.getString(2)) }
        }
        val map = HashMap<String, MutableSet<Long>>()
        c.prepareStatement("SELECT er.event_id, er.relay_id FROM event_relay er JOIN samples s ON s.event_id = er.event_id WHERE s.set_name = ?").use { ps ->
            ps.setString(1, set)
            ps.executeQuery().use { rs -> while (rs.next()) map.getOrPut(rs.getString(1)) { HashSet() }.add(rs.getLong(2)) }
        }
        return posts.map { (id, pk) -> PostReplicas(id, pk, map[id] ?: emptySet()) }
    }

    fun replication(set: String): String = db.read { c ->
        val posts = loadReplicas(c, set)
        if (posts.isEmpty()) return@read "## Replication\n\n_Sample set '$set' is empty._\n\n"
        val relaysQueried = c.queryLong("SELECT COUNT(*) FROM replicate_relays WHERE set_name = ? AND status = 'done'", set)
        val relaysFailed = c.queryLong("SELECT COUNT(*) FROM replicate_relays WHERE set_name = ? AND status NOT IN ('done')", set)
        val asOf = c.createStatement().use { st ->
            st.executeQuery("SELECT l.relay_id, MIN(i.asn) FROM latest_relay_ip l JOIN ip_info i ON i.ip = l.ip WHERE i.asn IS NOT NULL GROUP BY l.relay_id")
                .mapRows { Pair(it.getLong(1), it.getInt(2)) }.toMap()
        }
        val withList = c.createStatement().use { st -> st.executeQuery("SELECT pubkey FROM relay_list_authors WHERE n_write > 0").mapRows { it.getString(1) }.toHashSet() }
        val checked = c.createStatement().use { st -> st.executeQuery("SELECT pubkey FROM relay_list_checked").mapRows { it.getString(1) }.toHashSet() }
        val counts = posts.map { it.relays.size.toLong() }
        val asCounts = posts.map { p -> p.relays.mapNotNull { asOf[it] }.toSet().size.toLong() }
        val hist = linkedMapOf("1" to 0, "2–5" to 0, "6–10" to 0, "11–20" to 0, "21–50" to 0, ">50" to 0)
        for (n in counts) {
            val k = when { n <= 1 -> "1"; n <= 5 -> "2–5"; n <= 10 -> "6–10"; n <= 20 -> "11–20"; n <= 50 -> "21–50"; else -> ">50" }
            hist[k] = hist.getValue(k) + 1
        }
        val yes = posts.filter { it.pubkey in withList }.map { it.relays.size.toLong() }
        val no = posts.filter { it.pubkey in checked && it.pubkey !in withList }.map { it.relays.size.toLong() }
        buildString {
            appendLine("## Replication — sample '$set'")
            appendLine()
            appendLine("${posts.size} posts queried on $relaysQueried relays ($relaysFailed relays failed or were skipped). Paper (2023): mean 34.6 relays, 12.4 ASes per post.")
            appendLine()
            appendLine("| metric | relays per post | ASes per post |\n|---|---|---|")
            appendLine("| mean | ${fmt(counts.average())} | ${fmt(asCounts.average())} |")
            appendLine("| median | ${percentile(counts, 50)} | ${percentile(asCounts, 50)} |")
            appendLine("| p90 | ${percentile(counts, 90)} | ${percentile(asCounts, 90)} |")
            appendLine("| max | ${counts.max()} | ${asCounts.max()} |")
            appendLine()
            appendLine("| replicas | posts | share |\n|---|---|---|")
            hist.forEach { (k, n) -> appendLine("| $k | $n | ${pct(n, posts.size)} |") }
            appendLine()
            appendLine("NIP-65 split (authors checked: ${posts.count { it.pubkey in checked }} of ${posts.size}):")
            appendLine()
            appendLine("| author publishes kind-10002 | posts | mean replicas | median |\n|---|---|---|---|")
            appendLine("| yes | ${yes.size} | ${if (yes.isEmpty()) "-" else fmt(yes.average())} | ${percentile(yes, 50) ?: "-"} |")
            appendLine("| no | ${no.size} | ${if (no.isEmpty()) "-" else fmt(no.average())} | ${percentile(no, 50) ?: "-"} |")
            appendLine()
        }
    }

    // Remove the X relays holding the most sampled posts; what fraction is still on some relay?
    fun takedown(set: String): String = db.read { c ->
        val posts = loadReplicas(c, set)
        if (posts.isEmpty()) return@read ""
        val relayLoad = HashMap<Long, Int>()
        for (p in posts) for (r in p.relays) relayLoad[r] = (relayLoad[r] ?: 0) + 1
        val ranked = relayLoad.entries.sortedByDescending { it.value }.map { it.key }
        val names = c.createStatement().use { st -> st.executeQuery("SELECT id, url FROM relays").mapRows { Pair(it.getLong(1), it.getString(2)) }.toMap() }
        buildString {
            appendLine("## Takedown simulation — sample '$set'")
            appendLine()
            appendLine("Top relays by sampled posts held: " + ranked.take(5).joinToString(", ") { "${names[it]} (${relayLoad[it]})" })
            appendLine()
            appendLine("| top-X relays removed | posts still available | share |\n|---|---|---|")
            for (x in listOf(0, 1, 2, 3, 5, 10, 20, 30, 50)) {
                val removed = ranked.take(x).toHashSet()
                val alive = posts.count { p -> p.relays.any { it !in removed } }
                appendLine("| $x | $alive | ${pct(alive, posts.size)} |")
            }
            appendLine()
        }
    }

    private fun latestSet(c: Connection): String? = c.createStatement().use { st ->
        st.executeQuery("SELECT set_name FROM samples GROUP BY set_name ORDER BY MAX(rowid) DESC LIMIT 1").use { rs -> if (rs.next()) rs.getString(1) else null }
    }

    private fun pct(n: Int, d: Int): String = if (d == 0) "-" else String.format("%.1f%%", 100.0 * n / d)
    private fun pct(n: Int, d: Long): String = pct(n, d.toInt())
    private fun pct(n: Long, d: Long): String = pct(n.toInt(), d.toInt())
    private fun fmt(d: Double): String = String.format("%.1f", d)
}
