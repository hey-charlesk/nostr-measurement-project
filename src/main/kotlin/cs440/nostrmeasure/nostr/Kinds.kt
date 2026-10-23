package cs440.nostrmeasure.nostr

data class RelayListEntry(val url: String, val read: Boolean, val write: Boolean)

object Kinds {
    const val PROFILE = 0
    const val NOTE = 1
    const val FOLLOWS = 3
    const val ZAP_RECEIPT = 9735
    const val RELAY_LIST = 10002
    const val NIP66_RELAY = 30166

    // NIP-65: kind-10002 "r" tags, optional third element "read" | "write"; absent means both.
    fun parseRelayList(ev: Event): List<RelayListEntry> {
        if (ev.kind != RELAY_LIST) return emptyList()
        val out = LinkedHashMap<String, RelayListEntry>()
        for (t in ev.tags) {
            if (t.size < 2 || t[0] != "r") continue
            val url = RelayUrl.normalize(t[1]) ?: continue
            val marker = t.getOrNull(2)?.lowercase()
            val read = marker == null || marker == "read"
            val write = marker == null || marker == "write"
            val prev = out[url]
            out[url] = if (prev == null) RelayListEntry(url, read, write)
            else RelayListEntry(url, prev.read || read, prev.write || write)
        }
        return out.values.toList()
    }

    // Kind-3 "p" tags = followed pubkeys.
    fun parseFollows(ev: Event): List<String> =
        if (ev.kind != FOLLOWS) emptyList() else ev.tagValues("p").filter { it.isHex(64) }.distinct()

    // NIP-66 kind-30166: "d" tag is the relay URL; "n" tags list networks (clearnet, tor, ...).
    fun nip66RelayUrl(ev: Event): String? {
        if (ev.kind != NIP66_RELAY) return null
        val networks = ev.tagValues("n")
        if (networks.isNotEmpty() && networks.none { it.equals("clearnet", ignoreCase = true) }) return null
        return ev.firstTag("d")?.getOrNull(1)?.let { RelayUrl.normalize(it) }
    }
}
