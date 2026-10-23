package cs440.nostrmeasure.nostr

import org.json.JSONArray
import org.json.JSONObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

object Vectors {
    private val root: JSONObject by lazy {
        JSONObject(Vectors::class.java.getResource("/vectors.json")!!.readText())
    }

    fun events(key: String): List<Event> {
        val arr: JSONArray = root.getJSONArray(key)
        return (0 until arr.length()).map { Event.fromJson(arr.getJSONObject(it)) ?: error("bad vector $key[$it]") }
    }
}

class EventTest {
    @Test
    fun realEventsRecomputeToTheirOwnId() {
        val all = Vectors.events("k1") + Vectors.events("k10002") + Vectors.events("k30166")
        assertTrue(all.size >= 5)
        for (ev in all) assertEquals(ev.id, ev.computeId(), "id mismatch for kind ${ev.kind}")
    }

    @Test
    fun realEventsHaveValidSignatures() {
        for (ev in Vectors.events("k1") + Vectors.events("k10002")) assertTrue(ev.verify(), "bad sig for ${ev.id}")
    }

    @Test
    fun tamperedContentFailsVerification() {
        val ev = Vectors.events("k1").first()
        val tampered = ev.copy(content = ev.content + " ")
        assertFalse(tampered.idMatches())
        assertFalse(tampered.verify())
    }

    @Test
    fun escapingMatchesNip01() {
        // "/" must not be escaped, control characters must be.
        val ev = Event("", "a".repeat(64), 1, 1, listOf(listOf("t", "x/y")), "a/b\n\"c\"\u0001", "")
        val id = ev.computeId()
        val expectedPayload = "[0,\"${"a".repeat(64)}\",1,1,[[\"t\",\"x/y\"]],\"a/b\\n\\\"c\\\"\\u0001\"]"
        assertEquals(sha256(expectedPayload.toByteArray()).toHex(), id)
    }

    @Test
    fun fromJsonToleratesGarbage() {
        assertNotNull(Event.fromJson(Vectors.events("k1").first().toJson()))
        assertEquals(null, Event.fromJson(JSONObject("{\"id\":1}")))
    }
}
