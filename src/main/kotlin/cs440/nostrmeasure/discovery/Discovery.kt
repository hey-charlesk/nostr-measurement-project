package cs440.nostrmeasure.discovery

import cs440.nostrmeasure.Config
import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.nowSec
import cs440.nostrmeasure.db.todayUtc
import cs440.nostrmeasure.nostr.Filter
import cs440.nostrmeasure.nostr.Kinds
import cs440.nostrmeasure.nostr.Nip11
import cs440.nostrmeasure.nostr.Nip11Info
import cs440.nostrmeasure.nostr.RelayConn
import cs440.nostrmeasure.nostr.RelayUrl
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.io.File

private val log = KotlinLogging.logger {}

data class DiscoverySummary(val perSource: Map<String, Int>, val totalUrls: Int, val newRelays: Int, val nip11Fetched: Int, val nip11Ok: Int)

class Discovery(private val db: Db) {

    suspend fun run(fetchNip11: Boolean = true, nip11All: Boolean = false, sinceDays: Long = 7): DiscoverySummary {
        val found = LinkedHashMap<String, MutableSet<String>>()
        val perSource = LinkedHashMap<String, Int>()
        for (monitor in Config.nip66Monitors) {
            val urls = try { nip66(monitor, sinceDays) } catch (e: Exception) {
                log.warn { "NIP-66 monitor $monitor failed: ${e.message}" }; emptySet()
            }
            val src = "nip66:" + RelayUrl.host(monitor)
            perSource[src] = urls.size
            urls.forEach { found.getOrPut(it) { mutableSetOf() }.add(src) }
            archive("nip66-${RelayUrl.host(monitor)}", urls)
        }
        Config.seedRelays.mapNotNull { RelayUrl.normalize(it) }.forEach { found.getOrPut(it) { mutableSetOf() }.add("seed") }
        perSource["seed"] = Config.seedRelays.size

        val before = db.read { it.queryLongCount("relays") }
        val day = todayUtc()
        db.tx { c ->
            c.prepareStatement("INSERT OR IGNORE INTO relay_sightings(relay_id, source, day) VALUES(?,?,?)").use { ps ->
                for ((url, sources) in found) {
                    val id = db.relayId(c, url)
                    for (s in sources) { ps.setLong(1, id); ps.setString(2, s); ps.setString(3, day); ps.addBatch() }
                }
                ps.executeBatch()
            }
        }
        val after = db.read { it.queryLongCount("relays") }
        log.info { "discovery: ${found.size} urls from ${perSource.size} sources; relays ${before} -> ${after}" }

        var fetched = 0
        var ok = 0
        if (fetchNip11) {
            val targets = db.read { c ->
                val where = if (nip11All) "" else
                    "WHERE id NOT IN (SELECT relay_id FROM relay_snapshots WHERE fetched_at > ${nowSec() - 86_400})"
                db.relays(c, where)
            }
            log.info { "nip11: fetching ${targets.size} relays" }
            val results = fetchNip11All(targets)
            fetched = results.size
            ok = results.count { it.second.ok }
            db.tx { c -> results.forEach { (id, info) -> insertSnapshot(c, id, info) } }
            log.info { "nip11: $ok/$fetched ok" }
        }
        return DiscoverySummary(perSource, found.size, (after - before).toInt(), fetched, ok)
    }

    // Page through kind-30166 events from one monitor relay. These are addressable events
    // (one per relay URL), so a 7-day window with `until` pagination covers the monitor's view.
    suspend fun nip66(monitorUrl: String, sinceDays: Long): Set<String> {
        val since = nowSec() - sinceDays * 86_400
        val urls = LinkedHashSet<String>()
        RelayConn(Http.ws, monitorUrl).use { conn ->
            val cr = conn.connect(10_000)
            if (!cr.ok) { log.warn { "monitor $monitorUrl: ${cr.error} ${cr.message}" }; return emptySet() }
            var until = nowSec() + 60
            var pages = 0
            var events = 0
            while (pages < 50) {
                val f = Filter.kinds(Kinds.NIP66_RELAY).since(since).until(until).limit(5000).build()
                val rr = conn.req(f, 30_000)
                if (rr.rawEvents == 0) break
                events += rr.rawEvents
                for (ev in rr.events) Kinds.nip66RelayUrl(ev)?.let { urls.add(it) }
                val minCreated = rr.events.minOfOrNull { it.createdAt } ?: break
                if (!rr.ok && rr.error != cs440.nostrmeasure.nostr.ErrorClass.EOSE_TIMEOUT) break
                if (minCreated - 1 < since || minCreated >= until) break
                until = minCreated - 1
                pages++
            }
            log.info { "monitor $monitorUrl: $events events over ${pages + 1} pages, ${urls.size} clearnet relays" }
        }
        return urls
    }

    suspend fun fetchNip11All(targets: List<Pair<Long, String>>): List<Pair<Long, Nip11Info>> = coroutineScope {
        val sem = Semaphore(Config.nip11Concurrency)
        targets.map { (id, url) ->
            async(Dispatchers.IO) {
                sem.withPermit { Pair(id, Nip11.fetch(Http.http, url, Config.nip11TimeoutMs)) }
            }
        }.awaitAll()
    }

    private fun archive(name: String, urls: Collection<String>) {
        try {
            val dir = File(Config.rawDir).apply { mkdirs() }
            File(dir, "$name-${todayUtc()}.txt").writeText(urls.joinToString("\n"))
        } catch (e: Exception) {
            log.warn { "archive failed: ${e.message}" }
        }
    }

    companion object {
        fun insertSnapshot(c: java.sql.Connection, relayId: Long, info: Nip11Info, now: Long = nowSec()) {
            c.prepareStatement(
                "INSERT INTO relay_snapshots(relay_id, fetched_at, http_status, ok, name, pubkey, software, software_name, version, " +
                    "supported_nips, payment_required, auth_required, restricted_writes, max_subscriptions, max_filters, max_limit, " +
                    "max_message_length, fees_json, server_header, cf_ray, error, raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ).use { ps ->
                val l = info.limitation
                ps.setLong(1, relayId); ps.setLong(2, now); ps.setInt(3, info.httpStatus); ps.setInt(4, if (info.ok) 1 else 0)
                ps.setString(5, info.name); ps.setString(6, info.pubkey); ps.setString(7, info.software); ps.setString(8, info.softwareName)
                ps.setString(9, info.version); ps.setString(10, info.supportedNips.joinToString(","))
                ps.setObject(11, l?.paymentRequired?.let { if (it) 1 else 0 }); ps.setObject(12, l?.authRequired?.let { if (it) 1 else 0 })
                ps.setObject(13, l?.restrictedWrites?.let { if (it) 1 else 0 }); ps.setObject(14, l?.maxSubscriptions)
                ps.setObject(15, l?.maxFilters); ps.setObject(16, l?.maxLimit); ps.setObject(17, l?.maxMessageLength)
                ps.setString(18, info.feesJson); ps.setString(19, info.serverHeader); ps.setInt(20, if (info.cfRay) 1 else 0)
                ps.setString(21, info.error); ps.setString(22, info.rawJson?.take(20_000))
                ps.executeUpdate()
            }
        }
    }
}

private fun java.sql.Connection.queryLongCount(table: String): Long =
    createStatement().use { st -> st.executeQuery("SELECT COUNT(*) FROM $table").use { rs -> rs.next(); rs.getLong(1) } }
