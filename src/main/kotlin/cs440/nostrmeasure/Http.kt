package cs440.nostrmeasure

import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

object Http {
    private val base: OkHttpClient by lazy {
        val dispatcher = Dispatcher().apply {
            maxRequests = 1024
            maxRequestsPerHost = 64
        }
        OkHttpClient.Builder()
            .dispatcher(dispatcher)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(15, TimeUnit.SECONDS)
            .retryOnConnectionFailure(false)
            .followRedirects(true)
            .addInterceptor { chain ->
                chain.proceed(chain.request().newBuilder().header("User-Agent", Config.userAgent).build())
            }
            .build()
    }

    // Plain HTTP (NIP-11, ip-api).
    val http: OkHttpClient get() = base

    // WebSocket client: no read timeout, our own deadlines govern every subscription.
    val ws: OkHttpClient by lazy { base.newBuilder().readTimeout(0, TimeUnit.MILLISECONDS).build() }

    fun shutdown() {
        base.dispatcher.executorService.shutdown()
        base.connectionPool.evictAll()
    }
}
