package cs440.nostrmeasure

import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

// A scripted relay on MockWebServer. `onReq` decides what frames to send for each REQ.
class FakeRelay(
    private val httpStatus: Int = 101,
    private val onReq: (subId: String, filter: JSONObject, ws: WebSocket) -> Unit,
) : AutoCloseable {
    val server = MockWebServer()
    val reqs = CopyOnWriteArrayList<JSONObject>()
    val connections = AtomicInteger()
    val openSockets = AtomicInteger()

    val url: String get() = "ws://localhost:${server.port}"

    fun start(): FakeRelay {
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                connections.incrementAndGet()
                if (httpStatus != 101) return MockResponse().setResponseCode(httpStatus)
                return MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) { openSockets.incrementAndGet() }
                    override fun onMessage(webSocket: WebSocket, text: String) {
                        val arr = JSONArray(text)
                        if (arr.optString(0) == "REQ") {
                            val filter = arr.getJSONObject(2)
                            reqs.add(filter)
                            onReq(arr.getString(1), filter, webSocket)
                        }
                    }
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) { webSocket.close(code, null); openSockets.decrementAndGet() }
                })
            }
        }
        server.start()
        return this
    }

    override fun close() = server.shutdown()

    companion object {
        fun eventFrame(subId: String, ev: JSONObject): String = JSONArray().put("EVENT").put(subId).put(ev).toString()
        fun eoseFrame(subId: String): String = JSONArray().put("EOSE").put(subId).toString()
        fun closedFrame(subId: String, reason: String): String = JSONArray().put("CLOSED").put(subId).put(reason).toString()
        fun noticeFrame(msg: String): String = JSONArray().put("NOTICE").put(msg).toString()
    }
}
