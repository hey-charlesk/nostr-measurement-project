package cs440.nostrmeasure.nostr

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import java.io.EOFException
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl.SSLException

enum class ErrorClass {
    OK, DNS_FAIL, CONNECT_TIMEOUT, CONNECT_REFUSED, TLS_ERROR, HTTP_4XX, HTTP_5XX, CONNECT_ERROR,
    WS_CLOSED_EARLY, EOSE_TIMEOUT, AUTH_REQUIRED, RATE_LIMITED, NOT_CONNECTED,
}

data class ConnectResult(val ok: Boolean, val connectMs: Long, val error: ErrorClass, val message: String? = null, val httpStatus: Int? = null)

data class ReqResult(
    val events: List<Event>,
    val rawEvents: Int,
    val invalidEvents: Int,
    val eose: Boolean,
    val eoseMs: Long,
    val truncated: Boolean,
    val closedReason: String?,
    val notices: List<String>,
    val error: ErrorClass,
    val message: String? = null,
) {
    val ok: Boolean get() = error == ErrorClass.OK
}

data class StreamResult(val events: Int, val arrivalNanos: LongArray, val eoseMs: Long, val error: ErrorClass)

// One relay, one WebSocket, one subscription in flight at a time. No automatic reconnects:
// a measurement tool must see every failure, not paper over it.
class RelayConn(private val client: OkHttpClient, val url: String) : AutoCloseable {
    private val startNanos = System.nanoTime()
    private val opened = CompletableDeferred<ConnectResult>()
    private val reqMutex = Mutex()
    private var ws: WebSocket? = null
    @Volatile private var closed = false
    @Volatile private var active: ActiveReq? = null
    val notices = CopyOnWriteArrayList<String>()
    @Volatile var authChallenge: String? = null
        private set
    @Volatile var badFrames = 0
        private set

    private sealed class Done {
        data class Eose(val truncated: Boolean) : Done()
        data class Closed(val reason: String) : Done()
        data class Failed(val error: ErrorClass, val message: String?) : Done()
    }

    private class ActiveReq(val subId: String, val maxEvents: Int, val verify: Boolean, val streaming: Boolean) {
        val events = ArrayList<Event>()
        val arrivals = ArrayList<Long>()
        var raw = 0
        var invalid = 0
        var eoseAtNanos = 0L
        val done = CompletableDeferred<Done>()
    }

    suspend fun connect(timeoutMs: Long): ConnectResult {
        val req = Request.Builder().url(url).build()
        ws = client.newWebSocket(req, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                opened.complete(ConnectResult(true, elapsedMs(), ErrorClass.OK, httpStatus = response.code))
            }

            override fun onMessage(webSocket: WebSocket, text: String) = handle(text)

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                closed = true
                val cls = classify(t, response)
                opened.complete(ConnectResult(false, elapsedMs(), cls, t.message?.take(120), response?.code))
                active?.done?.complete(Done.Failed(ErrorClass.WS_CLOSED_EARLY, t.message?.take(120)))
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                webSocket.close(code, null)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                closed = true
                active?.done?.complete(Done.Failed(ErrorClass.WS_CLOSED_EARLY, "closed $code $reason".take(120)))
            }
        })
        val r = withTimeoutOrNull(timeoutMs) { opened.await() }
        if (r != null) return r
        ws?.cancel()
        closed = true
        return ConnectResult(false, elapsedMs(), ErrorClass.CONNECT_TIMEOUT, "no open within ${timeoutMs}ms")
    }

    suspend fun req(filter: org.json.JSONObject, timeoutMs: Long, maxEvents: Int = Int.MAX_VALUE, verify: Boolean = false): ReqResult =
        reqMutex.withLock {
            val socket = ws
            if (socket == null || closed) return ReqResult(emptyList(), 0, 0, false, 0, false, null, notices.toList(), ErrorClass.NOT_CONNECTED)
            val a = ActiveReq("nm" + SUB_COUNTER.incrementAndGet(), maxEvents, verify, streaming = false)
            active = a
            val t0 = System.nanoTime()
            socket.send(JSONArray().put("REQ").put(a.subId).put(filter).toString())
            val done = withTimeoutOrNull(timeoutMs) { a.done.await() }
            active = null
            if (!closed) socket.send(JSONArray().put("CLOSE").put(a.subId).toString())
            val ms = (System.nanoTime() - t0) / 1_000_000
            val ns = notices.toList()
            fun result(error: ErrorClass, eose: Boolean, truncated: Boolean, closedReason: String?, msg: String?) =
                ReqResult(a.events, a.raw, a.invalid, eose, ms, truncated, closedReason, ns, error, msg)
            when (done) {
                null -> result(ErrorClass.EOSE_TIMEOUT, false, false, null, "no EOSE within ${timeoutMs}ms")
                is Done.Eose -> result(ErrorClass.OK, true, done.truncated, null, null)
                is Done.Closed -> result(classifyReason(done.reason), false, false, done.reason, done.reason)
                is Done.Failed -> result(done.error, false, false, null, done.message)
            }
        }

    // Keep a subscription open for durationMs after EOSE and record arrival times of live events.
    suspend fun stream(filter: org.json.JSONObject, durationMs: Long, eoseTimeoutMs: Long): StreamResult = reqMutex.withLock {
        val socket = ws
        if (socket == null || closed) return StreamResult(0, LongArray(0), 0, ErrorClass.NOT_CONNECTED)
        val a = ActiveReq("nm" + SUB_COUNTER.incrementAndGet(), Int.MAX_VALUE, verify = false, streaming = true)
        active = a
        val t0 = System.nanoTime()
        socket.send(JSONArray().put("REQ").put(a.subId).put(filter).toString())
        val done = withTimeoutOrNull(durationMs) { a.done.await() }
        active = null
        if (!closed) socket.send(JSONArray().put("CLOSE").put(a.subId).toString())
        val eoseMs = if (a.eoseAtNanos > 0) (a.eoseAtNanos - t0) / 1_000_000 else -1
        val error = when {
            done is Done.Closed -> classifyReason(done.reason)
            done is Done.Failed -> done.error
            a.eoseAtNanos == 0L && eoseMs > eoseTimeoutMs -> ErrorClass.EOSE_TIMEOUT
            else -> ErrorClass.OK
        }
        StreamResult(a.raw, a.arrivals.toLongArray(), eoseMs, error)
    }

    override fun close() {
        closed = true
        try { ws?.close(1000, null) } catch (_: Exception) {}
    }

    fun cancel() {
        closed = true
        try { ws?.cancel() } catch (_: Exception) {}
    }

    private fun handle(text: String) {
        val a = active
        try {
            val arr = JSONArray(text)
            when (arr.optString(0)) {
                "EVENT" -> {
                    if (a == null || arr.optString(1) != a.subId) return
                    a.raw++
                    a.arrivals.add(System.nanoTime())
                    val ev = Event.fromJson(arr.getJSONObject(2))
                    if (ev == null || (a.verify && !ev.verify())) { a.invalid++; return }
                    if (!a.streaming) {
                        a.events.add(ev)
                        if (a.events.size >= a.maxEvents) a.done.complete(Done.Eose(truncated = true))
                    }
                }
                "EOSE" -> if (a != null && arr.optString(1) == a.subId) {
                    a.eoseAtNanos = System.nanoTime()
                    if (!a.streaming) a.done.complete(Done.Eose(truncated = false))
                }
                "CLOSED" -> if (a != null && arr.optString(1) == a.subId) a.done.complete(Done.Closed(arr.optString(2).take(200)))
                "NOTICE" -> {
                    val n = arr.optString(1).take(200)
                    notices.add(n)
                    if (a != null && looksRateLimited(n)) a.done.complete(Done.Closed("notice: $n"))
                }
                "AUTH" -> authChallenge = arr.optString(1)
                "OK", "COUNT" -> {}
                else -> badFrames++
            }
        } catch (e: Exception) {
            badFrames++
        }
    }

    private fun elapsedMs() = (System.nanoTime() - startNanos) / 1_000_000

    companion object {
        private val SUB_COUNTER = AtomicLong()

        fun classify(t: Throwable, response: Response?): ErrorClass {
            val code = response?.code
            if (code != null && code >= 500) return ErrorClass.HTTP_5XX
            if (code != null && code >= 400) return ErrorClass.HTTP_4XX
            return when (t) {
                is UnknownHostException -> ErrorClass.DNS_FAIL
                is SSLException -> ErrorClass.TLS_ERROR
                is SocketTimeoutException -> ErrorClass.CONNECT_TIMEOUT
                is ConnectException -> ErrorClass.CONNECT_REFUSED
                is EOFException -> ErrorClass.WS_CLOSED_EARLY
                else -> {
                    val m = t.message ?: ""
                    when {
                        m.contains("Expected HTTP 101") -> ErrorClass.HTTP_4XX
                        m.contains("timeout", ignoreCase = true) -> ErrorClass.CONNECT_TIMEOUT
                        m.contains("refused", ignoreCase = true) -> ErrorClass.CONNECT_REFUSED
                        m.contains("reset", ignoreCase = true) || m.contains("closed", ignoreCase = true) -> ErrorClass.WS_CLOSED_EARLY
                        else -> ErrorClass.CONNECT_ERROR
                    }
                }
            }
        }

        fun classifyReason(reason: String): ErrorClass = when {
            reason.contains("auth-required", ignoreCase = true) || reason.contains("restricted", ignoreCase = true) -> ErrorClass.AUTH_REQUIRED
            looksRateLimited(reason) -> ErrorClass.RATE_LIMITED
            else -> ErrorClass.WS_CLOSED_EARLY
        }

        private fun looksRateLimited(s: String): Boolean =
            s.contains("rate", ignoreCase = true) || s.contains("too many", ignoreCase = true) ||
                s.contains("slow down", ignoreCase = true) || s.contains("blocked", ignoreCase = true)
    }
}
