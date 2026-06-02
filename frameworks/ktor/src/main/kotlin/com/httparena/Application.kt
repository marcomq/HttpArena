package com.httparena

import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.http.content.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.compression.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.defaultheaders.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.utils.io.*
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.jetbrains.exposed.v1.core.between
import org.jetbrains.exposed.v1.r2dbc.selectAll
import org.jetbrains.exposed.v1.r2dbc.transactions.suspendTransaction
import java.io.File

fun main() {
    val appData = AppData()
    println("Ktor HttpArena server starting on :8080 (HTTP/1.1) and :8443 (HTTPS/HTTP+2)")

    val environment = applicationEnvironment {}
    val server = embeddedServer(Netty, environment, {
        enableHttp2 = true

        connector {
            port = 8080
            host = "0.0.0.0"
        }
        appData.keystore?.let { keyStore ->
            sslConnector(
                keyStore = keyStore,
                keyAlias = KEY_ALIAS,
                keyStorePassword = { KEYSTORE_PASSWORD },
                privateKeyPassword = { KEYSTORE_PASSWORD }
            ) {
                port = 8081
                host = "0.0.0.0"
            }
            sslConnector(
                keyStore = keyStore,
                keyAlias = KEY_ALIAS,
                keyStorePassword = { KEYSTORE_PASSWORD },
                privateKeyPassword = { KEYSTORE_PASSWORD }
            ) {
                port = 8443
                host = "0.0.0.0"
            }
        }
    }) {
        install(DefaultHeaders) {
            header("Server", "ktor")
        }
        install(Compression) {
            gzip()
        }
        install(ContentNegotiation) {
            json(appData.json)
        }
        install(WebSockets)

        configureRouting(appData)
    }
    server.start(wait = true)
}

private fun Application.configureRouting(appData: AppData) {
    fun ApplicationCall.sumQueryParams(): Long =
        request.queryParameters.entries().sumOf { (_, v) ->
            v.sumOf { it.toLongOrNull() ?: 0L }
        }

    routing {
        /**
         * Pipelined
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/pipelined/
         */
        get("/pipeline") {
            call.respondText("ok", ContentType.Text.Plain)
        }

        /**
         * Baseline 1.1
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/baseline/
         */
        get("/baseline11") {
            call.respondText(
                call.sumQueryParams().toString(),
                ContentType.Text.Plain
            )
        }

        /**
         * Baseline 1.1
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/baseline/
         */
        post("/baseline11") {
            val sum = call.sumQueryParams()
            val body = call.receiveText().trim().toLongOrNull() ?: run {
                call.respondText(sum.toString(), ContentType.Text.Plain)
                return@post
            }
            call.respondText(
                (sum + body).toString(),
                ContentType.Text.Plain
            )
        }

        /**
         * Baseline 2
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/baseline/
         */
        get("/baseline2") {
            call.respondText(
                call.sumQueryParams().toString(),
                ContentType.Text.Plain
            )
        }

        /**
         * JSON processing
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/json-processing/
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/json-tls/
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/json-compressed/
         */
        get("/json/{count}") {
            if (appData.dataset.isEmpty()) {
                call.respondText("Dataset not loaded", ContentType.Text.Plain, HttpStatusCode.InternalServerError)
                return@get
            }
            var count = call.pathParameters["count"]?.toIntOrNull() ?: 0
            if (count < 0) count = 0
            if (count > appData.dataset.size) count = appData.dataset.size
            val m = call.request.queryParameters["m"]?.toIntOrNull() ?: 1
            val processed = appData.dataset.take(count).map { d ->
                ProcessedItem(
                    id = d.id, name = d.name, category = d.category,
                    price = d.price, quantity = d.quantity, active = d.active,
                    tags = d.tags, rating = d.rating,
                    total = d.price.toLong() * d.quantity * m
                )
            }
            call.respond(JsonResponse(items = processed, count = count))
        }

        /**
         * Async DB
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/async-database/
         */
        get("/async-db") {
            val min = call.request.queryParameters["min"]?.toIntOrNull() ?: 10
            val max = call.request.queryParameters["max"]?.toIntOrNull() ?: 50
            val limit = (call.request.queryParameters["limit"]?.toIntOrNull() ?: 50).coerceIn(1, 50)
            try {
                val items = suspendTransaction(appData.postgres) {
                    with(ItemTable) {
                        selectAll()
                            .where { price.between(min, max) }
                            .limit(limit)
                            .map(::toDbItem)
                            .toList()
                    }
                }
                call.respond(
                    DbResponse(
                        items = items,
                        count = items.size
                    )
                )
            } catch (e: Exception) {
                log.error("Failed to load items from DB", e)
                call.respondBytes("{\"items\":[],\"count\":0}".toByteArray(), ContentType.Application.Json)
            }
        }

        /**
         * Upload 20MB
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/upload/
         */
        post("/upload") {
            val body = call.receiveChannel()
            val sink = DevNull.asByteWriteChannel()
            val totalBytes = try {
                body.copyTo(sink)
            } finally {
                sink.flushAndClose()
            }
            call.respondText(totalBytes.toString(), ContentType.Text.Plain)
        }

        /**
         * Static files
         * https://www.http-arena.com/docs/test-profiles/h1/isolated/static/
         */
        staticFiles("/static", File("/data/static")) {
            preCompressed(CompressedFileType.BROTLI, CompressedFileType.GZIP)
        }

        /**
         * Echo WebSocket
         * https://www.http-arena.com/docs/test-profiles/ws/
         */
        webSocket("/ws") {
            for (message in incoming)
                send(message)
        }

    }
}
