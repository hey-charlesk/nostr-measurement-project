package cs440.nostrmeasure.replicate

import cs440.nostrmeasure.Config
import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.getIntOrNull
import cs440.nostrmeasure.db.mapRows
import cs440.nostrmeasure.db.nowSec
import cs440.nostrmeasure.nostr.ErrorClass
import cs440.nostrmeasure.nostr.Event
import cs440.nostrmeasure.nostr.Filter
import cs440.nostrmeasure.nostr.Kinds
import cs440.nostrmeasure.nostr.RelayConn
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val log = KotlinLogging.logger {}

data class SampleSummary(val setName: String, val requested: Int, val sampled: Int, val perRelay: Map<String, Int>)
data class ReplicateSummary(val setName: String, val posts: Int, val relaysTried: Int, val relaysDone: Int, val relaysFailed: Int, val relaysBrokenIds: Int, val hits: Int)
data class RelayListSummary(val authors: Int, val checked: Int, val withList: Int)

class Replicate(private val db: Db, private val client: OkHttpClient = Http.ws) {

    // Draw recent kind-1 ids from several large relays so no single relay's view defines the sample.
    suspend fun sample(n: Int, setName: String, hours: Long = 24, seed: Long = 42, sources: List<String> = Config.sampleSourceRelays): SampleSummary {
        val since = nowSec() - hours * 3600
        val perRelayQuota = (n + sources.size - 1) / sources.size
        val found = LinkedHashMap<String, Pair<Event, String>>()
        val perRelay = LinkedHashMap<String, Int>()
        for (src in sources) {
            var got = 0
            RelayConn(client, src).use { conn ->
                val cr = conn.connect(10_000)
                if (!cr.ok) { log.warn { "sample source $src: ${cr.error}" }; perRelay[src] = 0; return@use }
                var until = nowSec() + 60
                var pages = 0
                while (got < perRelayQuota && pages < 100) {
                    val f = Filter.kinds(Kinds.NOTE).since(since).until(until).limit(minOf(500, perRelayQuota - got + 50)).build()
                    val rr = conn.req(f, 20_000)
                    if (rr.rawEvents == 0) break
                    for (ev in rr.events) {
                        if (ev.id.length == 64 && ev.createdAt in since..until && !found.containsKey(ev.id)) { found[ev.id] = Pair(ev, src); got++ }
                        if (got >= perRelayQuota) break
                    }
                    val minCreated = rr.events.minOfOrNull { it.createdAt } ?: break
                    if (minCreated <= since) break
                    until = minCreated - 1
                    pages++
                    if (!rr.ok) break
                }
            }
            perRelay[src] = got
        }
        val chosen = found.values.shuffled(Random(seed)).take(n)
        val now = nowSec()
        db.tx { c ->
            val insEvent = c.prepareStatement("INSERT OR IGNORE INTO events(id, pubkey, kind, created_at, first_relay_id, first_seen) VALUES(?,?,?,?,?,?)")
            val insSample = c.prepareStatement("INSERT OR IGNORE INTO samples(set_name, event_id) VALUES(?,?)")
            val insER = c.prepareStatement("INSERT OR IGNORE INTO event_relay(event_id, relay_id, via, seen_at) VALUES(?,?,?,?)")
            val relayIds = sources.associateWith { db.relayId(c, it, now) }
            for ((ev, src) in chosen) {
                val rid = relayIds.getValue(src)
                insEvent.setString(1, ev.id); insEvent.setString(2, ev.pubkey); insEvent.setInt(3, ev.kind); insEvent.setLong(4, ev.createdAt)
                insEvent.setLong(5, rid); insEvent.setLong(6, now); insEvent.addBatch()
                insSample.setString(1, setName); insSample.setString(2, ev.id); insSample.addBatch()
                insER.setString(1, ev.id); insER.setLong(2, rid); insER.setString(3, "sample"); insER.setLong(4, now); insER.addBatch()
            }
            insEvent.executeBatch(); insSample.executeBatch(); insER.executeBatch()
            insEvent.close(); insSample.close(); insER.close()
        }
        log.info { "sample '$setName': ${chosen.size} posts from ${perRelay}" }
        return SampleSummary(setName, n, chosen.size, perRelay)
    }

    data class Candidate(val relayId: Long, val url: String, val maxLimit: Int?)

    // Which relays to ask: everything that answered a REQ in the latest probe tick.
    fun candidates(): List<Candidate> = db.read { c ->
        c.createStatement().use { st ->
            st.executeQuery(
                "SELECT r.id, r.url, s.max_limit FROM latest_probe p JOIN relays r ON r.id = p.relay_id " +
                    "LEFT JOIN latest_snapshot s ON s.relay_id = r.id WHERE p.eose_ok = 1 ORDER BY r.id",
            ).mapRows { Candidate(it.getLong(1), it.getString(2), it.getIntOrNull("max_limit")) }
        }
    }

    suspend fun replicate(setName: String, concurrency: Int = Config.replicateConcurrency, batchOverride: Int? = null, relayFilter: String? = null): ReplicateSummary {
        val ids = db.read { c ->
            c.prepareStatement("SELECT event_id FROM samples WHERE set_name = ? ORDER BY event_id").use { ps -> ps.setString(1, setName); ps.executeQuery().mapRows { it.getString(1) } }
        }
        require(ids.isNotEmpty()) { "sample set '$setName' is empty; run `sample` first" }
        val idSet = ids.toHashSet()
        val cands = candidates().filter { relayFilter == null || it.url == relayFilter }
        log.info { "replicate '$setName': ${ids.size} posts x ${cands.size} relays" }
        val done = AtomicInteger(); val failed = AtomicInteger(); val broken = AtomicInteger(); val hits = AtomicInteger()
        coroutineScope {
            val sem = Semaphore(concurrency)
            for (cand in cands) {
                launch {
                    sem.withPermit {
                        val r = replicateOne(setName, cand, ids, idSet, batchOverride)
                        when (r.status) { "done" -> done.incrementAndGet(); "broken_ids" -> broken.incrementAndGet(); else -> failed.incrementAndGet() }
                        hits.addAndGet(r.hits)
                    }
                }
            }
        }
        log.info { "replicate '$setName': done=${done.get()} failed=${failed.get()} broken_ids=${broken.get()} hits=${hits.get()}" }
        return ReplicateSummary(setName, ids.size, cands.size, done.get(), failed.get(), broken.get(), hits.get())
    }

    data class RelayOutcome(val status: String, val hits: Int)

    private suspend fun replicateOne(setName: String, cand: Candidate, ids: List<String>, idSet: Set<String>, batchOverride: Int?): RelayOutcome {
        val batchSize = minOf(batchOverride ?: Config.replicateDefaultBatch, cand.maxLimit ?: Config.replicateDefaultBatch).coerceAtLeast(1)
        val batches = ids.chunked(batchSize)
        val start = db.read { c ->
            c.prepareStatement("SELECT batches_done, status FROM replicate_relays WHERE set_name = ? AND relay_id = ?").use { ps ->
                ps.setString(1, setName); ps.setLong(2, cand.relayId)
                ps.executeQuery().use { rs -> if (rs.next()) Pair(rs.getInt(1), rs.getString(2)) else Pair(0, "new") }
            }
        }
        if (start.second == "done" || start.second == "broken_ids") return RelayOutcome(start.second, 0)
        var hits = 0
        val conn = RelayConn(client, cand.url)
        try {
            val cr = conn.connect(10_000)
            if (!cr.ok) { record(setName, cand.relayId, "connect_failed", start.first, batches.size, 0, cr.error.name); return RelayOutcome("failed", 0) }
            var bi = start.first
            while (bi < batches.size) {
                val batch = batches[bi]
                val f = Filter.ids(batch).limit(batch.size).build()
                var rr = conn.req(f, Config.replicateEoseTimeoutMs)
                var retries = 0
                while (rr.error == ErrorClass.RATE_LIMITED && retries < 3) {
                    retries++
                    delay(10_000L * retries)
                    rr = conn.req(f, Config.replicateEoseTimeoutMs)
                }
                if (!rr.ok && rr.error != ErrorClass.EOSE_TIMEOUT) {
                    record(setName, cand.relayId, "failed", bi, batches.size, hits, "${rr.error} ${rr.message ?: ""}".take(120))
                    return RelayOutcome("failed", hits)
                }
                val requested = batch.toHashSet()
                val returned = rr.events.map { it.id }
                if (returned.any { it !in requested }) {
                    record(setName, cand.relayId, "broken_ids", bi, batches.size, hits, "returned ids outside the filter")
                    return RelayOutcome("broken_ids", hits)
                }
                val found = returned.filter { it in idSet }.distinct()
                hits += found.size
                val now = nowSec()
                db.tx { c ->
                    c.prepareStatement("INSERT OR IGNORE INTO event_relay(event_id, relay_id, via, seen_at) VALUES(?,?,?,?)").use { ps ->
                        for (id in found) { ps.setString(1, id); ps.setLong(2, cand.relayId); ps.setString(3, "replicate"); ps.setLong(4, now); ps.addBatch() }
                        ps.executeBatch()
                    }
                    upsertProgress(c, setName, cand.relayId, "running", bi + 1, batches.size, hits, null)
                }
                bi++
                if (bi < batches.size) delay(Config.replicateInterBatchMs)
            }
            record(setName, cand.relayId, "done", batches.size, batches.size, hits, null)
            return RelayOutcome("done", hits)
        } finally {
            conn.cancel()
        }
    }

    private fun record(setName: String, relayId: Long, status: String, done: Int, total: Int, hits: Int, note: String?) =
        db.tx { c -> upsertProgress(c, setName, relayId, status, done, total, hits, note) }

    private fun upsertProgress(c: java.sql.Connection, setName: String, relayId: Long, status: String, done: Int, total: Int, hits: Int, note: String?) {
        c.prepareStatement(
            "INSERT OR REPLACE INTO replicate_relays(set_name, relay_id, status, batches_done, batches_total, hits, note, updated_at) VALUES(?,?,?,?,?,?,?,?)",
        ).use { ps ->
            ps.setString(1, setName); ps.setLong(2, relayId); ps.setString(3, status); ps.setInt(4, done); ps.setInt(5, total)
            ps.setInt(6, hits); ps.setString(7, note); ps.setLong(8, nowSec()); ps.executeUpdate()
        }
    }

    // Kind-10002 relay lists for every author in the sample, newest event per author across the sources.
    suspend fun fetchRelayLists(setName: String, sources: List<String> = Config.relayListSourceRelays, recheck: Boolean = false): RelayListSummary {
        val authors = db.read { c ->
            val sql = "SELECT DISTINCT e.pubkey FROM samples s JOIN events e ON e.id = s.event_id WHERE s.set_name = ?" +
                (if (recheck) "" else " AND e.pubkey NOT IN (SELECT pubkey FROM relay_list_checked)")
            c.prepareStatement(sql).use { ps -> ps.setString(1, setName); ps.executeQuery().mapRows { it.getString(1) } }
        }
        val total = db.read { c ->
            c.prepareStatement("SELECT COUNT(DISTINCT e.pubkey) FROM samples s JOIN events e ON e.id = s.event_id WHERE s.set_name = ?").use { ps ->
                ps.setString(1, setName); ps.executeQuery().use { rs -> rs.next(); rs.getInt(1) }
            }
        }
        log.info { "relay-lists '$setName': ${authors.size} authors to check (of $total)" }
        val newest = HashMap<String, Event>()
        for (src in sources) {
            RelayConn(client, src).use { conn ->
                val cr = conn.connect(10_000)
                if (!cr.ok) { log.warn { "relay-list source $src: ${cr.error}" }; return@use }
                for (chunk in authors.chunked(100)) {
                    val rr = conn.req(Filter.kinds(Kinds.RELAY_LIST).authors(chunk).limit(500).build(), 20_000, verify = true)
                    for (ev in rr.events) {
                        val prev = newest[ev.pubkey]
                        if (prev == null || ev.createdAt > prev.createdAt) newest[ev.pubkey] = ev
                    }
                    delay(Config.replicateInterBatchMs)
                }
            }
        }
        val now = nowSec()
        db.tx { c ->
            val checked = c.prepareStatement("INSERT OR REPLACE INTO relay_list_checked(pubkey, checked_at) VALUES(?,?)")
            val delLists = c.prepareStatement("DELETE FROM relay_lists WHERE pubkey = ?")
            val insList = c.prepareStatement("INSERT OR REPLACE INTO relay_lists(pubkey, relay_url, read, write, created_at) VALUES(?,?,?,?,?)")
            val insAuthor = c.prepareStatement("INSERT OR REPLACE INTO relay_list_authors(pubkey, event_id, created_at, n_write, n_read, fetched_at) VALUES(?,?,?,?,?,?)")
            for (a in authors) { checked.setString(1, a); checked.setLong(2, now); checked.addBatch() }
            for ((pk, ev) in newest) {
                val entries = Kinds.parseRelayList(ev)
                delLists.setString(1, pk); delLists.addBatch()
                for (e in entries) {
                    insList.setString(1, pk); insList.setString(2, e.url); insList.setInt(3, if (e.read) 1 else 0); insList.setInt(4, if (e.write) 1 else 0)
                    insList.setLong(5, ev.createdAt); insList.addBatch()
                }
                insAuthor.setString(1, pk); insAuthor.setString(2, ev.id); insAuthor.setLong(3, ev.createdAt)
                insAuthor.setInt(4, entries.count { it.write }); insAuthor.setInt(5, entries.count { it.read }); insAuthor.setLong(6, now); insAuthor.addBatch()
            }
            checked.executeBatch(); delLists.executeBatch(); insList.executeBatch(); insAuthor.executeBatch()
            checked.close(); delLists.close(); insList.close(); insAuthor.close()
        }
        log.info { "relay-lists '$setName': ${newest.size}/${authors.size} authors publish a kind-10002 list" }
        return RelayListSummary(total, authors.size, newest.size)
    }
}
