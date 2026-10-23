package cs440.nostrmeasure.rate

import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.nowSec
import cs440.nostrmeasure.nostr.Filter
import cs440.nostrmeasure.nostr.RelayConn
import cs440.nostrmeasure.probe.percentile
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.OkHttpClient

private val log = KotlinLogging.logger {}

data class RateSummary(val url: String, val seconds: Long, val events: Int, val eventsPerSec: Double, val eoseMs: Long, val iaP50Ms: Long?, val iaP99Ms: Long?, val error: String)

// Passive observation: subscribe to live events and count what the relay pushes. This measures the
// relay's actual load as seen by one client, never its capacity.
class Rate(private val db: Db, private val client: OkHttpClient = Http.ws) {
    suspend fun observe(url: String, minutes: Int, kinds: IntArray): RateSummary {
        val startedAt = nowSec()
        val conn = RelayConn(client, url)
        try {
            val cr = conn.connect(10_000)
            if (!cr.ok) return RateSummary(url, 0, 0, 0.0, -1, null, null, "${cr.error} ${cr.message ?: ""}")
            val f = Filter().kinds(*kinds).since(startedAt).build()
            log.info { "observing $url for $minutes min (kinds ${kinds.joinToString(",")})" }
            val sr = conn.stream(f, minutes * 60_000L, 15_000)
            val seconds = minutes * 60L
            val ia = sr.arrivalNanos.toList().zipWithNext { a, b -> (b - a) / 1_000_000 }
            val summary = RateSummary(
                url, seconds, sr.events, sr.events.toDouble() / seconds, sr.eoseMs,
                percentile(ia, 50), percentile(ia, 99), sr.error.name,
            )
            db.tx { c ->
                val rid = db.relayId(c, url)
                c.prepareStatement("INSERT OR REPLACE INTO rate_obs(relay_id, started_at, seconds, kinds, events, eose_ms, ia_p50_ms, ia_p99_ms) VALUES(?,?,?,?,?,?,?,?)").use { ps ->
                    ps.setLong(1, rid); ps.setLong(2, startedAt); ps.setLong(3, seconds); ps.setString(4, kinds.joinToString(","))
                    ps.setInt(5, sr.events); ps.setLong(6, sr.eoseMs); ps.setObject(7, summary.iaP50Ms); ps.setObject(8, summary.iaP99Ms)
                    ps.executeUpdate()
                }
            }
            return summary
        } finally {
            conn.cancel()
        }
    }
}
