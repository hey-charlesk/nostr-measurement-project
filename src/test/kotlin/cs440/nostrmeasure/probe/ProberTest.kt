package cs440.nostrmeasure.probe

import cs440.nostrmeasure.FakeRelay
import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.nostr.ErrorClass
import cs440.nostrmeasure.nostr.Vectors
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProberTest {
    private fun tempDb() = Db(Files.createTempDirectory("nm").resolve("t.db").toString())
    private val note = Vectors.events("k1").first().toJson()

    private fun prober(db: Db) = Prober(db, Http.ws, concurrency = 4, connectTimeoutMs = 2_000, eoseTimeoutMs = 700, tickDeadlineMs = 10_000)

    @Test
    fun healthyRelayIsOk() = runBlocking {
        FakeRelay { sub, _, ws -> ws.send(FakeRelay.eventFrame(sub, note)); ws.send(FakeRelay.eoseFrame(sub)) }.start().use { relay ->
            tempDb().use { db ->
                val r = prober(db).probeOne(1, relay.url)
                assertTrue(r.wsOk); assertTrue(r.eoseOk)
                assertEquals(ErrorClass.OK, r.errorClass)
                assertEquals(1, r.events)
                assertTrue(r.connectMs!! >= 0 && r.eoseMs!! >= 0)
            }
        }
    }

    @Test
    fun silentRelayIsEoseTimeout() = runBlocking {
        FakeRelay { _, _, _ -> }.start().use { relay ->
            tempDb().use { db ->
                val r = prober(db).probeOne(1, relay.url)
                assertTrue(r.wsOk); assertFalse(r.eoseOk)
                assertEquals(ErrorClass.EOSE_TIMEOUT, r.errorClass)
            }
        }
    }

    @Test
    fun authRequiredIsClassified() = runBlocking {
        FakeRelay { sub, _, ws -> ws.send(FakeRelay.closedFrame(sub, "auth-required: this relay needs NIP-42")) }.start().use { relay ->
            tempDb().use { db ->
                val r = prober(db).probeOne(1, relay.url)
                assertTrue(r.wsOk); assertFalse(r.eoseOk)
                assertEquals(ErrorClass.AUTH_REQUIRED, r.errorClass)
            }
        }
    }

    @Test
    fun rateLimitNoticeIsClassified() = runBlocking {
        FakeRelay { _, _, ws -> ws.send(FakeRelay.noticeFrame("rate limited: slow down")) }.start().use { relay ->
            tempDb().use { db ->
                val r = prober(db).probeOne(1, relay.url)
                assertEquals(ErrorClass.RATE_LIMITED, r.errorClass)
            }
        }
    }

    @Test
    fun http503IsNotUp() = runBlocking {
        FakeRelay(httpStatus = 503) { _, _, _ -> }.start().use { relay ->
            tempDb().use { db ->
                val r = prober(db).probeOne(1, relay.url)
                assertFalse(r.wsOk)
                assertEquals(ErrorClass.HTTP_5XX, r.errorClass)
            }
        }
    }

    @Test
    fun refusedPortIsNotUp() = runBlocking {
        tempDb().use { db ->
            val r = prober(db).probeOne(1, "ws://127.0.0.1:1")
            assertFalse(r.wsOk)
            assertTrue(r.errorClass == ErrorClass.CONNECT_REFUSED || r.errorClass == ErrorClass.CONNECT_ERROR, r.errorClass.name)
        }
    }

    @Test
    fun tickWritesOneRowPerRelayAndATickRow() = runBlocking {
        FakeRelay { sub, _, ws -> ws.send(FakeRelay.eoseFrame(sub)) }.start().use { relay ->
            tempDb().use { db ->
                val ids = db.tx { c -> listOf(db.relayId(c, relay.url), db.relayId(c, "ws://127.0.0.1:1")) }
                val s = prober(db).tick(ids.zip(listOf(relay.url, "ws://127.0.0.1:1")))
                assertEquals(2, s.probed); assertEquals(1, s.wsOk)
                val rows = db.read { c -> c.createStatement().use { st -> st.executeQuery("SELECT COUNT(*) FROM probes").use { rs -> rs.next(); rs.getInt(1) } } }
                val ticks = db.read { c -> c.createStatement().use { st -> st.executeQuery("SELECT probed, ws_ok FROM probe_ticks").use { rs -> rs.next(); Pair(rs.getInt(1), rs.getInt(2)) } } }
                assertEquals(2, rows); assertEquals(Pair(2, 1), ticks)
            }
        }
    }
}
