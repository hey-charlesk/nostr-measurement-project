package cs440.nostrmeasure.nostr

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KindsTest {
    private fun ev(kind: Int, tags: List<List<String>>) = Event("x", "y", 0, kind, tags, "", "")

    @Test
    fun relayListMarkers() {
        val e = ev(10002, listOf(
            listOf("r", "wss://a.example/"),
            listOf("r", "wss://b.example", "read"),
            listOf("r", "wss://c.example", "write"),
            listOf("r", "wss://c.example", "read"),
            listOf("r", "wss://onion.onion"),
            listOf("p", "deadbeef"),
        ))
        val list = Kinds.parseRelayList(e).associateBy { it.url }
        assertEquals(3, list.size)
        assertEquals(RelayListEntry("wss://a.example", read = true, write = true), list["wss://a.example"])
        assertEquals(RelayListEntry("wss://b.example", read = true, write = false), list["wss://b.example"])
        assertEquals(RelayListEntry("wss://c.example", read = true, write = true), list["wss://c.example"])
    }

    @Test
    fun realRelayListsParse() {
        for (e in Vectors.events("k10002")) {
            val list = Kinds.parseRelayList(e)
            assertTrue(list.isNotEmpty())
            assertTrue(list.all { it.url.startsWith("ws") })
        }
    }

    @Test
    fun nip66() {
        for (e in Vectors.events("k30166")) assertTrue(Kinds.nip66RelayUrl(e)!!.startsWith("wss://"))
        assertEquals("wss://r.example", Kinds.nip66RelayUrl(ev(30166, listOf(listOf("d", "wss://r.example/"), listOf("n", "clearnet")))))
        assertNull(Kinds.nip66RelayUrl(ev(30166, listOf(listOf("d", "wss://r.example"), listOf("n", "tor")))))
        assertNull(Kinds.nip66RelayUrl(ev(1, listOf(listOf("d", "wss://r.example")))))
    }

    @Test
    fun follows() {
        val pk = "ab".repeat(32)
        assertEquals(listOf(pk), Kinds.parseFollows(ev(3, listOf(listOf("p", pk), listOf("p", pk), listOf("p", "short")))))
    }
}
