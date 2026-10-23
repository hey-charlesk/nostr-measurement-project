package cs440.nostrmeasure.nostr

import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.util.concurrent.TimeUnit

data class Limitation(
    val paymentRequired: Boolean?,
    val authRequired: Boolean?,
    val restrictedWrites: Boolean?,
    val maxSubscriptions: Int?,
    val maxFilters: Int?,
    val maxLimit: Int?,
    val maxMessageLength: Int?,
)

data class Nip11Info(
    val httpStatus: Int,
    val name: String?,
    val pubkey: String?,
    val software: String?,
    val softwareName: String?,
    val version: String?,
    val supportedNips: List<Int>,
    val limitation: Limitation?,
    val feesJson: String?,
    val rawJson: String?,
    val serverHeader: String?,
    val cfRay: Boolean,
    val error: String?,
) {
    val ok: Boolean get() = rawJson != null && error == null
}

object Nip11 {
    fun url(relayUrl: String): String = when {
        relayUrl.startsWith("wss://") -> "https://" + relayUrl.removePrefix("wss://")
        relayUrl.startsWith("ws://") -> "http://" + relayUrl.removePrefix("ws://")
        else -> relayUrl
    }

    // "git+https://github.com/hoytech/strfry.git" -> "strfry"; "https://github.com/x/nostr-rs-relay" -> "nostr-rs-relay"
    fun softwareName(software: String?): String? {
        val s = software?.trim()?.trimEnd('/') ?: return null
        if (s.isEmpty()) return null
        val last = s.substringAfterLast('/').substringAfterLast(':').removeSuffix(".git").lowercase()
        return last.ifEmpty { s.lowercase() }.take(64)
    }

    fun parse(json: String, httpStatus: Int, serverHeader: String?, cfRay: Boolean): Nip11Info {
        val o = try { JSONObject(json) } catch (e: Exception) {
            return failed(httpStatus, serverHeader, cfRay, "bad json: ${e.message?.take(80)}")
        }
        val lim = o.optJSONObject("limitation")?.let {
            Limitation(
                paymentRequired = it.optBooleanOrNull("payment_required"),
                authRequired = it.optBooleanOrNull("auth_required"),
                restrictedWrites = it.optBooleanOrNull("restricted_writes"),
                maxSubscriptions = it.optIntOrNull("max_subscriptions"),
                maxFilters = it.optIntOrNull("max_filters"),
                maxLimit = it.optIntOrNull("max_limit"),
                maxMessageLength = it.optIntOrNull("max_message_length"),
            )
        }
        val nips = o.optJSONArray("supported_nips")?.let { arr ->
            (0 until arr.length()).mapNotNull { i -> arr.opt(i)?.toString()?.toIntOrNull() }
        } ?: emptyList()
        val software = o.optStringOrNull("software")
        return Nip11Info(
            httpStatus = httpStatus,
            name = o.optStringOrNull("name"),
            pubkey = o.optStringOrNull("pubkey"),
            software = software,
            softwareName = softwareName(software),
            version = o.optStringOrNull("version"),
            supportedNips = nips,
            limitation = lim,
            feesJson = o.optJSONObject("fees")?.toString(),
            rawJson = json,
            serverHeader = serverHeader,
            cfRay = cfRay,
            error = null,
        )
    }

    fun fetch(client: OkHttpClient, relayUrl: String, timeoutMs: Long = 5_000): Nip11Info {
        val c = client.newBuilder().callTimeout(timeoutMs, TimeUnit.MILLISECONDS).build()
        val req = Request.Builder().url(url(relayUrl)).header("Accept", "application/nostr+json").build()
        return try {
            c.newCall(req).execute().use { resp ->
                val server = resp.header("server")
                val cfRay = resp.header("cf-ray") != null
                val body = resp.body?.string() ?: ""
                if (!resp.isSuccessful) failed(resp.code, server, cfRay, "http ${resp.code}")
                else parse(body, resp.code, server, cfRay)
            }
        } catch (e: Exception) {
            failed(0, null, false, (e::class.simpleName ?: "error") + ": " + (e.message?.take(80) ?: ""))
        }
    }

    private fun failed(status: Int, server: String?, cfRay: Boolean, error: String) = Nip11Info(
        httpStatus = status, name = null, pubkey = null, software = null, softwareName = null, version = null,
        supportedNips = emptyList(), limitation = null, feesJson = null, rawJson = null,
        serverHeader = server, cfRay = cfRay, error = error,
    )

    private fun JSONObject.optStringOrNull(key: String): String? =
        if (has(key) && !isNull(key)) opt(key)?.toString()?.take(512) else null

    private fun JSONObject.optBooleanOrNull(key: String): Boolean? =
        if (has(key) && !isNull(key)) (opt(key) as? Boolean) else null

    private fun JSONObject.optIntOrNull(key: String): Int? =
        if (has(key) && !isNull(key)) (opt(key) as? Number)?.toInt() else null
}
