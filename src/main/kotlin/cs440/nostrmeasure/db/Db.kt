package cs440.nostrmeasure.db

import cs440.nostrmeasure.nostr.RelayUrl
import org.sqlite.SQLiteConfig
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

fun nowSec(): Long = System.currentTimeMillis() / 1000
fun todayUtc(): String = LocalDate.now(ZoneOffset.UTC).toString()

// One SQLite connection per process, serialized by a lock. Writers batch their rows into a
// single transaction so the prober and the crawler never hold the lock for long.
class Db(val path: String) : AutoCloseable {
    private val conn: Connection
    private val lock = ReentrantLock()

    init {
        File(path).absoluteFile.parentFile?.mkdirs()
        val cfg = SQLiteConfig().apply {
            setJournalMode(SQLiteConfig.JournalMode.WAL)
            setBusyTimeout(30_000)
            setSynchronous(SQLiteConfig.SynchronousMode.NORMAL)
        }
        conn = DriverManager.getConnection("jdbc:sqlite:$path", cfg.toProperties())
        val schema = Db::class.java.getResource("/schema.sql")?.readText() ?: error("schema.sql missing")
        conn.createStatement().use { st ->
            schema.split(";").map { it.trim() }.filter { it.isNotEmpty() }.forEach { st.execute(it) }
        }
    }

    fun <T> tx(block: (Connection) -> T): T = lock.withLock {
        conn.autoCommit = false
        try {
            val r = block(conn)
            conn.commit()
            r
        } catch (e: Exception) {
            conn.rollback()
            throw e
        } finally {
            conn.autoCommit = true
        }
    }

    fun <T> read(block: (Connection) -> T): T = lock.withLock { block(conn) }

    // Upsert a relay by canonical URL inside an open transaction and return its id.
    fun relayId(c: Connection, url: String, now: Long = nowSec()): Long {
        c.prepareStatement(
            "INSERT INTO relays(url, host, first_seen, last_seen) VALUES(?,?,?,?) " +
                "ON CONFLICT(url) DO UPDATE SET last_seen = excluded.last_seen",
        ).use { ps ->
            ps.setString(1, url); ps.setString(2, RelayUrl.host(url)); ps.setLong(3, now); ps.setLong(4, now)
            ps.executeUpdate()
        }
        c.prepareStatement("SELECT id FROM relays WHERE url = ?").use { ps ->
            ps.setString(1, url)
            ps.executeQuery().use { rs -> rs.next(); return rs.getLong(1) }
        }
    }

    fun relays(c: Connection, where: String = "", args: List<Any> = emptyList()): List<Pair<Long, String>> =
        c.prepareStatement("SELECT id, url FROM relays $where ORDER BY id").use { ps ->
            args.forEachIndexed { i, a -> ps.setObject(i + 1, a) }
            ps.executeQuery().mapRows { Pair(it.getLong(1), it.getString(2)) }
        }

    override fun close() = conn.close()
}

fun <T> ResultSet.mapRows(f: (ResultSet) -> T): List<T> = use {
    val out = ArrayList<T>()
    while (next()) out.add(f(this))
    out
}

fun Connection.queryLong(sql: String, vararg args: Any?): Long = prepareStatement(sql).use { ps ->
    args.forEachIndexed { i, a -> ps.setObject(i + 1, a) }
    ps.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else 0L }
}

fun ResultSet.getLongOrNull(col: String): Long? = getLong(col).let { if (wasNull()) null else it }
fun ResultSet.getIntOrNull(col: String): Int? = getInt(col).let { if (wasNull()) null else it }
