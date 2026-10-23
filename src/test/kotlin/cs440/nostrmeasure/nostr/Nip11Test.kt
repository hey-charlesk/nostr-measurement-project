package cs440.nostrmeasure.nostr

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class Nip11Test {
    private val strfry = """{"contact":"jb55@jb55.com","description":"Damus strfry relay","limitation":{"max_limit":500,"max_message_length":1000000,"max_subscriptions":200},"name":"damus.io","negentropy":1,"pubkey":"32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245","software":"git+https://github.com/hoytech/strfry.git","supported_nips":[1,2,4,9,11,28,40,45,59,70],"version":"1.1.0-158-gb705403ddf49"}"""
    private val paid = """{"name":"nostr.wine","software":"https://github.com/example/nostr-rs-relay","limitation":{"payment_required":true,"auth_required":false,"max_limit":1000,"max_filters":10},"fees":{"admission":[{"amount":18888000,"unit":"msats"}]}}"""

    @Test
    fun parsesStrfry() {
        val i = Nip11.parse(strfry, 200, "cloudflare", cfRay = true)
        assertTrue(i.ok)
        assertEquals("damus.io", i.name)
        assertEquals("strfry", i.softwareName)
        assertEquals(500, i.limitation?.maxLimit)
        assertEquals(200, i.limitation?.maxSubscriptions)
        assertNull(i.limitation?.paymentRequired)
        assertTrue(45 in i.supportedNips)
        assertTrue(i.cfRay)
        assertEquals("cloudflare", i.serverHeader)
    }

    @Test
    fun parsesPaidRelay() {
        val i = Nip11.parse(paid, 200, null, cfRay = false)
        assertEquals("nostr-rs-relay", i.softwareName)
        assertEquals(true, i.limitation?.paymentRequired)
        assertEquals(false, i.limitation?.authRequired)
        assertEquals(10, i.limitation?.maxFilters)
        assertTrue(i.feesJson!!.contains("admission"))
    }

    @Test
    fun badJsonIsNotOk() {
        val i = Nip11.parse("<html>", 200, "nginx", cfRay = false)
        assertFalse(i.ok)
        assertTrue(i.error!!.startsWith("bad json"))
    }

    @Test
    fun urlAndSoftwareName() {
        assertEquals("https://relay.damus.io", Nip11.url("wss://relay.damus.io"))
        assertEquals("http://relay.example.com:8080/x", Nip11.url("ws://relay.example.com:8080/x"))
        assertEquals("khatru", Nip11.softwareName("https://github.com/fiatjaf/khatru"))
        assertEquals("nostream", Nip11.softwareName("nostream"))
        assertNull(Nip11.softwareName("  "))
    }
}
