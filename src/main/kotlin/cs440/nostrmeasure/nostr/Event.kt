package cs440.nostrmeasure.nostr

import fr.acinq.secp256k1.Secp256k1
import org.json.JSONArray
import org.json.JSONObject

data class Event(
    val id: String,
    val pubkey: String,
    val createdAt: Long,
    val kind: Int,
    val tags: List<List<String>>,
    val content: String,
    val sig: String,
) {
    fun tagValues(name: String): List<String> =
        tags.filter { it.size >= 2 && it[0] == name }.map { it[1] }

    fun firstTag(name: String): List<String>? = tags.firstOrNull { it.isNotEmpty() && it[0] == name }

    // NIP-01: id = sha256 of the UTF-8 serialization of [0, pubkey, created_at, kind, tags, content]
    // with no whitespace and a fixed escaping rule. org.json escapes "/" which NIP-01 does not,
    // so the serialization is done by hand.
    fun computeId(): String {
        val sb = StringBuilder(256 + content.length)
        sb.append("[0,\"").append(pubkey).append("\",").append(createdAt).append(',').append(kind).append(",[")
        tags.forEachIndexed { i, tag ->
            if (i > 0) sb.append(',')
            sb.append('[')
            tag.forEachIndexed { j, s ->
                if (j > 0) sb.append(',')
                appendJsonString(sb, s)
            }
            sb.append(']')
        }
        sb.append("],")
        appendJsonString(sb, content)
        sb.append(']')
        return sha256(sb.toString().toByteArray(Charsets.UTF_8)).toHex()
    }

    fun idMatches(): Boolean = id.isHex(64) && computeId() == id

    fun verify(): Boolean = try {
        idMatches() && sig.isHex(128) && pubkey.isHex(64) &&
            Secp256k1.verifySchnorr(sig.hexToBytes(), id.hexToBytes(), pubkey.hexToBytes())
    } catch (e: Exception) {
        false
    }

    fun toJson(): JSONObject = JSONObject()
        .put("id", id).put("pubkey", pubkey).put("created_at", createdAt).put("kind", kind)
        .put("tags", JSONArray(tags.map { JSONArray(it) })).put("content", content).put("sig", sig)

    companion object {
        fun fromJson(o: JSONObject): Event? = try {
            val tagsArr = o.getJSONArray("tags")
            val tags = ArrayList<List<String>>(tagsArr.length())
            for (i in 0 until tagsArr.length()) {
                val t = tagsArr.getJSONArray(i)
                tags.add(List(t.length()) { j -> t.get(j).toString() })
            }
            Event(
                id = o.getString("id"),
                pubkey = o.getString("pubkey"),
                createdAt = o.getLong("created_at"),
                kind = o.getInt("kind"),
                tags = tags,
                content = o.getString("content"),
                sig = o.optString("sig", ""),
            )
        } catch (e: Exception) {
            null
        }

        private fun appendJsonString(sb: StringBuilder, s: String) {
            sb.append('"')
            for (c in s) {
                when (c) {
                    '"' -> sb.append("\\\"")
                    '\\' -> sb.append("\\\\")
                    '\n' -> sb.append("\\n")
                    '\r' -> sb.append("\\r")
                    '\t' -> sb.append("\\t")
                    '\b' -> sb.append("\\b")
                    '\u000C' -> sb.append("\\f")
                    else -> if (c < ' ') sb.append(String.format("\\u%04x", c.code)) else sb.append(c)
                }
            }
            sb.append('"')
        }
    }
}
