package cs440.nostrmeasure.nostr

import org.json.JSONArray
import org.json.JSONObject

// Minimal NIP-01 filter builder.
class Filter {
    private val o = JSONObject()

    fun ids(v: Collection<String>) = apply { o.put("ids", JSONArray(v)) }
    fun authors(v: Collection<String>) = apply { o.put("authors", JSONArray(v)) }
    fun kinds(vararg k: Int) = apply { o.put("kinds", JSONArray(k.toList())) }
    fun since(t: Long) = apply { o.put("since", t) }
    fun until(t: Long) = apply { o.put("until", t) }
    fun limit(n: Int) = apply { o.put("limit", n) }
    fun tag(name: String, v: Collection<String>) = apply { o.put("#$name", JSONArray(v)) }

    fun build(): JSONObject = o

    companion object {
        fun kinds(vararg k: Int) = Filter().kinds(*k)
        fun ids(v: Collection<String>) = Filter().ids(v)
    }
}
