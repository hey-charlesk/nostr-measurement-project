package cs440.nostrmeasure.nostr

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RelayUrlTest {
    @Test
    fun canonicalizes() {
        assertEquals("wss://relay.damus.io", RelayUrl.normalize("wss://relay.damus.io/"))
        assertEquals("wss://relay.damus.io", RelayUrl.normalize("WSS://Relay.Damus.IO:443/"))
        assertEquals("wss://relay.example.com/nostr", RelayUrl.normalize("wss://relay.example.com/nostr/"))
        assertEquals("wss://relay.example.com:8443", RelayUrl.normalize("wss://relay.example.com:8443"))
        assertEquals("ws://relay.example.com", RelayUrl.normalize("ws://relay.example.com:80/"))
    }

    @Test
    fun rejectsNonPublic() {
        assertNull(RelayUrl.normalize("https://relay.damus.io"))
        assertNull(RelayUrl.normalize("wss://abc.onion"))
        assertNull(RelayUrl.normalize("wss://localhost:7777"))
        assertNull(RelayUrl.normalize("wss://192.168.1.10"))
        assertNull(RelayUrl.normalize("wss://10.0.0.1:4848"))
        assertNull(RelayUrl.normalize("wss://user:pw@relay.example.com"))
        assertNull(RelayUrl.normalize("wss://relay"))
        assertNull(RelayUrl.normalize("not a url"))
    }
}
