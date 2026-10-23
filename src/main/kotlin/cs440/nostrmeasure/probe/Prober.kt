package cs440.nostrmeasure.probe

import cs440.nostrmeasure.Config
import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.nowSec
import cs440.nostrmeasure.nostr.ErrorClass
import cs440.nostrmeasure.nostr.Filter
import cs440.nostrmeasure.nostr.Kinds
import cs440.nostrmeasure.nostr.RelayConn
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.util.concurrent.ConcurrentLinkedQueue

private val log = KotlinLogging.logger {}

data class ProbeRow(
    val relayId: Long,
    val url: String,
    val wsOk: Boolean,
    val eoseOk: Boolean,
    val connectMs: Long?,
    val eoseMs: Long?,
    val errorClass: ErrorClass,
    val errorMsg: String?,
    val events: Int,
)

data class TickSummary(
    val ts: Long,
    val total: Int,
    val probed: Int,
    val wsOk: Int,
    val eoseOk: Int,
    val byClass: Map<ErrorClass, Int>,
    val connectP50: Long?,
    val connectP95: Long?,
    val eoseP50: Long?,
    val eoseP95: Long?,
    val durationMs: Long,
)

class Prober(
    private val db: Db,
    private val client: OkHttpClient = Http.ws,
    private val concurrency: Int = Config.probeConcurrency,
    private val connectTimeoutMs: Long = Config.probeConnectTimeoutMs,
    private val eoseTimeoutMs: Long = Config.probeEoseTimeoutMs,
    private val tickDeadlineMs: Long = Config.probeTickDeadlineMs,
) {
    // The probe a normal client would make: open the socket, ask for one recent note, wait for EOSE.
    suspend fun probeOne(relayId: Long, url: String): ProbeRow {
        val conn = RelayConn(client, url)
        try {
            val cr = conn.connect(connectTimeoutMs)
            if (!cr.ok) return ProbeRow(relayId, url, false, false, null, null, cr.error, cr.message, 0)
            val rr = conn.req(Filter.kinds(Kinds.NOTE).limit(1).build(), eoseTimeoutMs, maxEvents = 1)
            val ok = rr.ok
            val msg = rr.message ?: rr.closedReason ?: rr.notices.firstOrNull()
            return ProbeRow(relayId, url, true, ok, cr.connectMs, if (ok) rr.eoseMs else null, rr.error, msg?.take(120), rr.rawEvents)
        } finally {
            conn.cancel()
        }
    }

    suspend fun tick(relays: List<Pair<Long, String>>): TickSummary {
        val ts = nowSec()
        val t0 = System.nanoTime()
        val results = ConcurrentLinkedQueue<ProbeRow>()
        val finished = withTimeoutOrNull(tickDeadlineMs) {
            coroutineScope {
                val sem = Semaphore(concurrency)
                for ((id, url) in relays) {
                    launch { sem.withPermit { results.add(probeOne(id, url)) } }
                }
            }
            true
        }
        if (finished == null) log.warn { "tick $ts hit the ${tickDeadlineMs / 60_000}-minute deadline with ${results.size}/${relays.size} probed" }
        val rows = results.toList()
        val durationMs = (System.nanoTime() - t0) / 1_000_000
        db.tx { c ->
            c.prepareStatement(
                "INSERT OR REPLACE INTO probes(relay_id, ts, ws_ok, eose_ok, connect_ms, eose_ms, error_class, error_msg, events_returned) VALUES(?,?,?,?,?,?,?,?,?)",
            ).use { ps ->
                for (r in rows) {
                    ps.setLong(1, r.relayId); ps.setLong(2, ts); ps.setInt(3, if (r.wsOk) 1 else 0); ps.setInt(4, if (r.eoseOk) 1 else 0)
                    ps.setObject(5, r.connectMs); ps.setObject(6, r.eoseMs); ps.setString(7, r.errorClass.name)
                    ps.setString(8, if (r.errorClass == ErrorClass.OK) null else r.errorMsg); ps.setInt(9, r.events)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            c.prepareStatement("INSERT OR REPLACE INTO probe_ticks(ts, relays_total, probed, ws_ok, duration_ms) VALUES(?,?,?,?,?)").use { ps ->
                ps.setLong(1, ts); ps.setInt(2, relays.size); ps.setInt(3, rows.size); ps.setInt(4, rows.count { it.wsOk }); ps.setLong(5, durationMs)
                ps.executeUpdate()
            }
        }
        val summary = TickSummary(
            ts = ts, total = relays.size, probed = rows.size,
            wsOk = rows.count { it.wsOk }, eoseOk = rows.count { it.eoseOk },
            byClass = rows.groupingBy { it.errorClass }.eachCount().toSortedMap(compareBy { it.name }),
            connectP50 = percentile(rows.mapNotNull { it.connectMs }, 50), connectP95 = percentile(rows.mapNotNull { it.connectMs }, 95),
            eoseP50 = percentile(rows.mapNotNull { it.eoseMs }, 50), eoseP95 = percentile(rows.mapNotNull { it.eoseMs }, 95),
            durationMs = durationMs,
        )
        log.info { "tick $ts: ${summary.wsOk}/${summary.total} up, ${summary.eoseOk} answered; connect p50=${summary.connectP50}ms eose p50=${summary.eoseP50}ms; ${durationMs / 1000}s" }
        return summary
    }

    fun loadRelays(relayFilter: String? = null): List<Pair<Long, String>> = db.read { c ->
        if (relayFilter == null) db.relays(c) else db.relays(c, "WHERE url = ?", listOf(relayFilter))
    }

    // Ticks aligned to wall-clock multiples of the interval so gaps are easy to spot later.
    suspend fun loop(intervalMinutes: Int, relayFilter: String? = null) {
        val interval = intervalMinutes * 60_000L
        while (true) {
            val now = System.currentTimeMillis()
            val next = (now / interval + 1) * interval
            log.info { "next tick in ${(next - now) / 1000}s" }
            delay(next - now)
            try {
                tick(loadRelays(relayFilter))
            } catch (e: Exception) {
                log.error(e) { "tick failed" }
            }
        }
    }
}

fun percentile(values: List<Long>, p: Int): Long? {
    if (values.isEmpty()) return null
    val sorted = values.sorted()
    val idx = ((p / 100.0) * (sorted.size - 1)).toInt().coerceIn(0, sorted.size - 1)
    return sorted[idx]
}
