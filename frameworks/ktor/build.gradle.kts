plugins {
    kotlin("jvm") version "2.3.0"
    kotlin("plugin.serialization") version "2.3.0"
    id("io.ktor.plugin") version "3.4.1"
    application
}

group = "com.httparena"
version = "1.0.0"

application {
    mainClass.set("com.httparena.ApplicationKt")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(ktorLibs.server.core)
    implementation(ktorLibs.server.netty)
    implementation(ktorLibs.server.compression)
    implementation(ktorLibs.server.defaultHeaders)
    implementation(ktorLibs.server.contentNegotiation)
    implementation(ktorLibs.serialization.kotlinx.json)
    implementation(ktorLibs.server.websockets)

    implementation(libs.exposed.core)
    implementation(libs.exposed.r2dbc)
    implementation(libs.exposed.json)
    implementation(libs.postgresql)
    implementation(libs.logback.classic)
}

ktor {
    fatJar {
        archiveFileName.set("ktor-httparena.jar")
    }
}

kotlin {
    jvmToolchain(21)
}
