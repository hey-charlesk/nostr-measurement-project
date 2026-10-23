package cs440.nostrmeasure.nostr

import java.net.URI

object RelayUrl {
    private val PRIVATE_V4 = listOf(
        Regex("""^10\."""), Regex("""^127\."""), Regex("""^0\."""), Regex("""^169\.254\."""),
        Regex("""^192\.168\."""), Regex("""^172\.(1[6-9]|2\d|3[01])\."""),
    )

    // Returns the canonical form (lowercase scheme/host, no default port, no trailing slash)
    // or null when the URL is not a public clearnet relay.
    fun normalize(raw: String): String? {
        val s = raw.trim()
        if (s.length > 255) return null
        val uri = try { URI(s) } catch (e: Exception) { return null }
        val scheme = uri.scheme?.lowercase() ?: return null
        if (scheme != "wss" && scheme != "ws") return null
        val host = uri.host?.lowercase()?.trimEnd('.') ?: return null
        if (host.isEmpty() || host == "localhost") return null
        if (host.endsWith(".onion") || host.endsWith(".i2p") || host.endsWith(".local") || host.endsWith(".internal")) return null
        if (host.startsWith("[") || host.contains(':')) return null // IPv6 literals: skip for now
        if (PRIVATE_V4.any { it.containsMatchIn(host) }) return null
        if (!host.contains('.')) return null
        if (uri.userInfo != null) return null
        val port = uri.port
        val defaultPort = if (scheme == "wss") 443 else 80
        val portPart = if (port == -1 || port == defaultPort) "" else ":$port"
        var path = uri.rawPath ?: ""
        path = path.trimEnd('/')
        return "$scheme://$host$portPart$path"
    }

    fun host(url: String): String = URI(url).host
}
