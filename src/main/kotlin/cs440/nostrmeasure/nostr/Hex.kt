package cs440.nostrmeasure.nostr

import java.security.MessageDigest

private val HEX = "0123456789abcdef".toCharArray()

fun ByteArray.toHex(): String {
    val out = CharArray(size * 2)
    for (i in indices) {
        val v = this[i].toInt() and 0xff
        out[i * 2] = HEX[v ushr 4]
        out[i * 2 + 1] = HEX[v and 0x0f]
    }
    return String(out)
}

fun String.hexToBytes(): ByteArray {
    require(length % 2 == 0) { "odd-length hex" }
    val out = ByteArray(length / 2)
    for (i in out.indices) {
        out[i] = ((Character.digit(this[i * 2], 16) shl 4) or Character.digit(this[i * 2 + 1], 16)).toByte()
    }
    return out
}

fun String.isHex(len: Int): Boolean = length == len && all { it in '0'..'9' || it in 'a'..'f' }

fun sha256(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(data)
