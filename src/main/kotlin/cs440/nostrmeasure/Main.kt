package cs440.nostrmeasure

import cs440.nostrmeasure.db.Db
import cs440.nostrmeasure.discovery.Discovery
import cs440.nostrmeasure.geo.Geo
import cs440.nostrmeasure.nostr.Nip11
import cs440.nostrmeasure.nostr.RelayUrl
import cs440.nostrmeasure.probe.Prober
import cs440.nostrmeasure.rate.Rate
import cs440.nostrmeasure.replicate.Replicate
import cs440.nostrmeasure.report.Report
import kotlinx.coroutines.runBlocking
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.system.exitProcess

private const val USAGE = """nostr-measure <command> [--flag=value ...]

  discover   [--nip11=true] [--all]            NIP-66 relay discovery + NIP-11 info documents
  nip11      <wss://relay>                       print one relay's NIP-11 document (parsed)
  probe      [--relay=wss://..] [--interval=15]  availability probe of every known relay; --interval loops forever
  geo                                            resolve relay hosts, map IPs to AS/country (ip-api)
  sample     [--n=300] [--set=NAME] [--hours=24] draw recent kind-1 ids from large relays into a sample set
  replicate  [--set=NAME] [--batch=100] [--concurrency=50] [--relay=wss://..]
                                                 ask every reachable relay which sampled ids it holds
  relay-lists [--set=NAME] [--recheck]           fetch kind-10002 (NIP-65) lists for the sampled authors
  rate       --relay=wss://.. [--minutes=5] [--kinds=1]
                                                 subscribe live and count events/s (passive)
  report     [--set=NAME] [--out=DIR]            print Markdown tables and write them to out/
  status                                         one-line summary

Environment: NOSTR_MEASURE_DB (default data/nostr-measure.db), NOSTR_MEASURE_CONTACT (goes in the User-Agent).
"""

fun main(args: Array<String>) {
    if (args.isEmpty() || args[0] == "help" || args[0] == "--help") { print(USAGE); return }
    val cmd = args[0]
    val positional = args.drop(1).filter { !it.startsWith("--") }
    val flags = args.drop(1).filter { it.startsWith("--") }.associate {
        val kv = it.removePrefix("--"); kv.substringBefore('=') to (if (kv.contains('=')) kv.substringAfter('=') else "true")
    }
    fun flag(name: String): String? = flags[name]
    fun int(name: String, default: Int) = flag(name)?.toIntOrNull() ?: default

    val code = try {
        runBlocking {
            when (cmd) {
                "nip11" -> {
                    val url = positional.firstOrNull() ?: flag("relay") ?: error("usage: nip11 <wss://relay>")
                    val info = Nip11.fetch(Http.http, RelayUrl.normalize(url) ?: url)
                    println(info.copy(rawJson = null))
                    println(info.rawJson ?: "(no document: ${info.error})")
                    0
                }
                "discover" -> Db(Config.dbPath).use { db ->
                    val s = Discovery(db).run(fetchNip11 = flag("nip11") != "false", nip11All = flag("all") == "true")
                    println("sources: ${s.perSource}; urls: ${s.totalUrls}; new relays: ${s.newRelays}; nip11 ok: ${s.nip11Ok}/${s.nip11Fetched}")
                    0
                }
                "probe" -> Db(Config.dbPath).use { db ->
                    val relay = flag("relay")?.let { RelayUrl.normalize(it) ?: error("bad relay url") }
                    if (relay != null) db.tx { c -> db.relayId(c, relay) }
                    val prober = Prober(db)
                    val interval = flag("interval")?.removeSuffix("m")?.toIntOrNull()
                    if (interval != null) prober.loop(interval, relay)
                    else {
                        val s = prober.tick(prober.loadRelays(relay))
                        println("tick ${s.ts}: ${s.wsOk}/${s.total} sockets opened, ${s.eoseOk} answered; by outcome: ${s.byClass}; connect p50/p95 ${s.connectP50}/${s.connectP95} ms; eose p50/p95 ${s.eoseP50}/${s.eoseP95} ms; ${s.durationMs / 1000}s")
                    }
                    0
                }
                "geo" -> Db(Config.dbPath).use { db ->
                    val s = Geo(db).run()
                    println("relays ${s.relays}, resolved ${s.resolved}, ips looked up ${s.ipsLookedUp}, failures ${s.ipFailures}")
                    0
                }
                "sample" -> Db(Config.dbPath).use { db ->
                    val s = Replicate(db).sample(int("n", 300), flag("set") ?: defaultSetName(), int("hours", 24).toLong())
                    println("sample '${s.setName}': ${s.sampled}/${s.requested} posts; per source ${s.perRelay}")
                    0
                }
                "replicate" -> Db(Config.dbPath).use { db ->
                    val set = flag("set") ?: db.read { latestSet(it) } ?: error("no sample set; run `sample` first")
                    val s = Replicate(db).replicate(set, int("concurrency", Config.replicateConcurrency), flag("batch")?.toIntOrNull(), flag("relay"))
                    println("replicate '${s.setName}': ${s.posts} posts, relays tried ${s.relaysTried}, done ${s.relaysDone}, failed ${s.relaysFailed}, broken-ids ${s.relaysBrokenIds}, hits ${s.hits}")
                    0
                }
                "relay-lists" -> Db(Config.dbPath).use { db ->
                    val set = flag("set") ?: db.read { latestSet(it) } ?: error("no sample set; run `sample` first")
                    val s = Replicate(db).fetchRelayLists(set, recheck = flag("recheck") == "true")
                    println("relay-lists: ${s.withList} of ${s.checked} newly checked authors publish NIP-65 lists (${s.authors} authors in set)")
                    0
                }
                "rate" -> Db(Config.dbPath).use { db ->
                    val url = flag("relay")?.let { RelayUrl.normalize(it) } ?: error("usage: rate --relay=wss://.. [--minutes=5]")
                    val kinds = (flag("kinds") ?: "1").split(',').mapNotNull { it.trim().toIntOrNull() }.toIntArray()
                    val s = Rate(db).observe(url, int("minutes", 5), kinds)
                    println("${s.url}: ${s.events} events in ${s.seconds}s = ${String.format("%.3f", s.eventsPerSec)}/s; EOSE ${s.eoseMs} ms; inter-arrival p50 ${s.iaP50Ms} ms p99 ${s.iaP99Ms} ms; ${s.error}")
                    0
                }
                "report" -> Db(Config.dbPath).use { db ->
                    val md = Report(db).full(flag("set"))
                    print(md)
                    val dir = File(flag("out") ?: Config.outDir).apply { mkdirs() }
                    val f = File(dir, "report-${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmm"))}.md")
                    f.writeText(md)
                    System.err.println("wrote ${f.path}")
                    0
                }
                "status" -> Db(Config.dbPath).use { db ->
                    print(Report(db).inventory()); print(Report(db).latestTick())
                    0
                }
                else -> { System.err.println("unknown command '$cmd'\n"); print(USAGE); 2 }
            }
        }
    } catch (e: Exception) {
        System.err.println("error: ${e.message}")
        1
    }
    Http.shutdown()
    exitProcess(code)
}

private fun defaultSetName(): String = "s" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmm"))

private fun latestSet(c: java.sql.Connection): String? = c.createStatement().use { st ->
    st.executeQuery("SELECT set_name FROM samples GROUP BY set_name ORDER BY MAX(rowid) DESC LIMIT 1").use { rs -> if (rs.next()) rs.getString(1) else null }
}
