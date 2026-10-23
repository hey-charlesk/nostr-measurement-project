plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

group = "cs440"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.okhttp)
    implementation(libs.json)
    implementation(libs.coroutines.core)
    implementation(libs.secp256k1.jvm)
    runtimeOnly(libs.secp256k1.jni.jvm)
    implementation(libs.sqlite.jdbc)
    implementation(libs.kotlin.logging)
    runtimeOnly(libs.logback)

    testImplementation(kotlin("test"))
    testImplementation(libs.okhttp.mockwebserver)
    testImplementation(libs.coroutines.test)
    testRuntimeOnly(libs.logback)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("cs440.nostrmeasure.MainKt")
    applicationDefaultJvmArgs = listOf("-Xmx1g", "-Dnetworkaddress.cache.ttl=600")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}
