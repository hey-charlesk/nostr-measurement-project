package cs440.nostrmeasure.replicate

import cs440.nostrmeasure.FakeRelay
import cs440.nostrmeasure.Http
import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.db.mapRows
import cs440.nostrmeasure.nostr.Vectors
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReplicateTest {
    private fun tempDb() = Db(Files.createTempDirectory("nm").resolve("t.db").toString())

    // A relay that "holds" a subset of events and answers ids filters honestly, honoring limit.
    private fun holdingRelay(held: List<JSONObject>) = FakeRelay { sub, filter, ws ->
        val ids = filter.optJSONArray("ids")?.let { a -> (0 until a.length()).map { a.getString(it) }.toSet() } ?: emptySet()
        val limit = filter.optInt("limit", Int.MAX_VALUE)
        held.filter { it.getString("id") in ids }.take(limit).forEach { ws.send(FakeRelay.eventFrame(sub, it)) }
        ws.send(FakeRelay.eoseFrame(sub))
    }.start()

    private fun seed(db: Db, relayUrl: String, maxLimit: Int?, sampleIds: List<Pair<String, String>>, setName: String): Long = db.tx { c ->
        val rid = db.relayId(c, relayUrl)
        c.createStatement().use { st ->
            st.execute("INSERT INTO probe_ticks(ts, relays_total, probed, ws_ok, duration_ms) VALUES(100, 1, 1, 1, 10)")
            st.execute("INSERT INTO probes(relay_id, ts, ws_ok, eose_ok, connect_ms, eose_ms, error_class) VALUES($rid, 100, 1, 1, 5, 5, 'OK')")
            st.execute("INSERT INTO relay_snapshots(relay_id, fetched_at, ok, max_limit) VALUES($rid, 100, 1, ${maxLimit ?: "NULL"})")
            for ((id, pk) in sampleIds) {
                st.execute("INSERT INTO events(id, pubkey, kind, created_at, first_seen) VALUES('$id', '$pk', 1, 1, 1)")
                st.execute("INSERT INTO samples(set_name, event_id) VALUES('$setName', '$id')")
            }
        }
        rid
    }

    @Test
    fun batchesByMaxLimitAndRecordsHits() = runBlocking {
        val events = Vectors.events("k1").map { it.toJson() }
        assertTrue(events.size >= 3)
        val held = events.take(2)
        holdingRelay(held).use { relay ->
            tempDb().use { db ->
                val rid = seed(db, relay.url, maxLimit = 2, sampleIds = events.map { it.getString("id") to it.getString("pubkey") }, setName = "t")
                val s = Replicate(db, Http.ws).replicate("t", concurrency = 2)
                assertEquals(1, s.relaysDone); assertEquals(0, s.relaysFailed)
                assertEquals(2, s.hits)
                assertEquals(2, relay.reqs.size, "3 ids with max_limit 2 must take 2 REQs")
                assertTrue(relay.reqs.all { it.getJSONArray("ids").length() <= 2 })
                val rows = db.read { c -> c.createStatement().use { st -> st.executeQuery("SELECT event_id FROM event_relay WHERE relay_id = $rid AND via = 'replicate'").mapRows { it.getString(1) } } }
                assertEquals(held.map { it.getString("id") }.toSet(), rows.toSet())
                val status = db.read { c -> c.createStatement().use { st -> st.executeQuery("SELECT status, batches_done, batches_total FROM replicate_relays").use { rs -> rs.next(); Triple(rs.getString(1), rs.getInt(2), rs.getInt(3)) } } }
                assertEquals(Triple("done", 2, 2), status)
            }
        }
    }

    @Test
    fun finishedRelayIsNotQueriedAgain() = runBlocking {
        val events = Vectors.events("k1").map { it.toJson() }
        holdingRelay(events).use { relay ->
            tempDb().use { db ->
                seed(db, relay.url, maxLimit = null, sampleIds = events.map { it.getString("id") to it.getString("pubkey") }, setName = "t")
                val r = Replicate(db, Http.ws)
                r.replicate("t"); val n = relay.reqs.size
                r.replicate("t")
                assertEquals(n, relay.reqs.size)
            }
        }
    }

    @Test
    fun relayReturningForeignIdsIsFlagged() = runBlocking {
        val events = Vectors.events("k1").map { it.toJson() }
        val foreign = Vectors.events("k10002").first().toJson()
        FakeRelay { sub, _, ws -> ws.send(FakeRelay.eventFrame(sub, foreign)); ws.send(FakeRelay.eoseFrame(sub)) }.start().use { relay ->
            tempDb().use { db ->
                seed(db, relay.url, maxLimit = null, sampleIds = events.map { it.getString("id") to it.getString("pubkey") }, setName = "t")
                val s = Replicate(db, Http.ws).replicate("t")
                assertEquals(1, s.relaysBrokenIds); assertEquals(0, s.hits)
            }
        }
    }
}
