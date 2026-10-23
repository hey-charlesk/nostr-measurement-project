package cs440.nostrmeasure.geo

import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.mapRows
import cs440.nostrmeasure.db.nowSec
import cs440.nostrmeasure.db.todayUtc
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.net.Inet4Address
import java.net.InetAddress

private val log = KotlinLogging.logger {}

data class GeoSummary(val relays: Int, val resolved: Int, val ipsLookedUp: Int, val ipFailures: Int)

class Geo(private val db: Db) {
    // ip-api.com free tier: HTTP only, 100 IPs per batch, 15 batches per minute.
    private val batchUrl = "http://ip-api.com/batch?fields=status,message,countryCode,as,asname,org,isp,hosting,query"

    suspend fun run(refreshDays: Long = 7): GeoSummary {
        val relays = db.read { c ->
            c.createStatement().use { st -> st.executeQuery("SELECT id, host FROM relays").mapRows { Pair(it.getLong(1), it.getString(2)) } }
        }
        val day = todayUtc()
        val resolved = resolveAll(relays)
        db.tx { c ->
            c.prepareStatement("INSERT OR IGNORE INTO relay_ip(relay_id, ip, day) VALUES(?,?,?)").use { ps ->
                for ((id, ips) in resolved) for (ip in ips) { ps.setLong(1, id); ps.setString(2, ip); ps.setString(3, day); ps.addBatch() }
                ps.executeBatch()
            }
        }
        val missing = db.read { c ->
            c.prepareStatement(
                "SELECT DISTINCT r.ip FROM relay_ip r LEFT JOIN ip_info i ON i.ip = r.ip WHERE i.ip IS NULL OR i.fetched_at < ?",
            ).use { ps -> ps.setLong(1, nowSec() - refreshDays * 86_400); ps.executeQuery().mapRows { it.getString(1) } }
        }
        log.info { "geo: ${resolved.count { it.second.isNotEmpty() }}/${relays.size} hosts resolved, ${missing.size} ips to look up" }
        var failures = 0
        val chunks = missing.chunked(100)
        chunks.forEachIndexed { i, chunk ->
            val rows = withContext(Dispatchers.IO) { lookup(chunk) }
            failures += rows.count { it.error != null }
            db.tx { c ->
                c.prepareStatement(
                    "INSERT OR REPLACE INTO ip_info(ip, asn, as_name, org, isp, country, hosting, fetched_at, error) VALUES(?,?,?,?,?,?,?,?,?)",
                ).use { ps ->
                    for (r in rows) {
                        ps.setString(1, r.ip); ps.setObject(2, r.asn); ps.setString(3, r.asName); ps.setString(4, r.org); ps.setString(5, r.isp)
                        ps.setString(6, r.country); ps.setObject(7, r.hosting?.let { if (it) 1 else 0 }); ps.setLong(8, nowSec()); ps.setString(9, r.error)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
            }
            if (i < chunks.size - 1) delay(4_100)
        }
        return GeoSummary(relays.size, resolved.count { it.second.isNotEmpty() }, missing.size, failures)
    }

    private suspend fun resolveAll(relays: List<Pair<Long, String>>): List<Pair<Long, List<String>>> = coroutineScope {
        val sem = Semaphore(64)
        relays.map { (id, host) ->
            async(Dispatchers.IO) {
                sem.withPermit {
                    val ips = try {
                        InetAddress.getAllByName(host).filterIsInstance<Inet4Address>().map { it.hostAddress }.distinct()
                    } catch (e: Exception) { emptyList() }
                    Pair(id, ips)
                }
            }
        }.awaitAll()
    }

    data class IpRow(val ip: String, val asn: Int?, val asName: String?, val org: String?, val isp: String?, val country: String?, val hosting: Boolean?, val error: String?)

    private fun lookup(ips: List<String>): List<IpRow> {
        val body = JSONArray(ips.map { JSONObject().put("query", it) }).toString()
        val req = Request.Builder().url(batchUrl).post(body.toRequestBody("application/json".toMediaType())).build()
        return try {
            Http.http.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return ips.map { IpRow(it, null, null, null, null, null, null, "http ${resp.code}") }
                val arr = JSONArray(resp.body?.string() ?: "[]")
                (0 until arr.length()).map { i ->
                    val o = arr.getJSONObject(i)
                    val ip = o.optString("query", ips.getOrElse(i) { "" })
                    if (o.optString("status") != "success") IpRow(ip, null, null, null, null, null, null, o.optString("message", "fail"))
                    else {
                        val asField = o.optString("as", "")
                        val asn = Regex("^AS(\\d+)").find(asField)?.groupValues?.get(1)?.toIntOrNull()
                        IpRow(
                            ip, asn, o.optString("asname", null) ?: asField.substringAfter(' ', ""), o.optString("org", null),
                            o.optString("isp", null), o.optString("countryCode", null), o.optBoolean("hosting", false), null,
                        )
                    }
                }
            }
        } catch (e: Exception) {
            log.warn { "ip-api batch failed: ${e.message}" }
            ips.map { IpRow(it, null, null, null, null, null, null, e.message?.take(80)) }
        }
    }

    companion object {
        // ASNs whose presence in front of a relay hides the origin host.
        val CDN_ASNS = setOf(13335, 20940, 16625, 54113, 209242)
    }
}
