package com.httparena

import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.io.Buffer
import kotlinx.io.RawSink
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.v1.r2dbc.R2dbcDatabase
import java.io.File
import java.net.URI
import java.security.KeyFactory
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

object DevNull : RawSink {
    override fun close() {}
    override fun flush() {}
    override fun write(source: Buffer, byteCount: Long) {}
}

const val CERT_PATH = "/certs/server.crt"
const val KEY_PATH = "/certs/server.key"
const val KEY_ALIAS = "server"
val KEYSTORE_PASSWORD = CharArray(0)

class AppData {
    private val certFile = File(CERT_PATH)
    private val keyFile = File(KEY_PATH)
    private val datasetFile = File(System.getenv("DATASET_PATH") ?: "/data/dataset.json")

    val json = Json { ignoreUnknownKeys = true }

    /**
     * Dataset from file.  Used in JSON endpoints.
     */
    var dataset: List<DatasetItem> = datasetFile.takeIf { it.exists() }?.let {
        json.decodeFromString(it.readText())
    } ?: emptyList()

    /**
     * PostgreSQL connection.  Used in async database endpoints.
     */
    val postgres: R2dbcDatabase? = System.getenv("DATABASE_URL")?.let { dbUrl ->
        runCatching {
            val uri = URI(dbUrl.replace("postgres://", "postgresql://"))
            val host = uri.host
            val port = if (uri.port > 0) uri.port else 5432
            val database = uri.path.removePrefix("/")
            val userInfo = uri.userInfo.split(":")
            R2dbcDatabase.connect {
                setUrl("r2dbc:postgresql://$host:$port/$database")
                connectionFactoryOptions {
                    option(ConnectionFactoryOptions.DRIVER, "postgresql")
                    option(ConnectionFactoryOptions.USER, userInfo[0])
                    option(ConnectionFactoryOptions.PASSWORD, if (userInfo.size > 1) userInfo[1] else "")
                }
            }
        }
    }?.getOrNull()

    /**
     * Keystore for TLS.  Used in JSON TLS and JSON compressed endpoints.
     */
    val keystore: KeyStore? = certFile.takeIf { it.exists() }?.let { certFile ->
        val certs = CertificateFactory.getInstance("X.509")
            .generateCertificates(certFile.inputStream())
            .map { it as X509Certificate }
            .toTypedArray()

        val keyBytes = Base64.getMimeDecoder().decode(
            keyFile.readText()
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("\\s".toRegex(), "")
        )
        val privateKey = KeyFactory.getInstance("RSA")
            .generatePrivate(PKCS8EncodedKeySpec(keyBytes))

        KeyStore.getInstance("PKCS12").apply {
            load(null, null)
            setKeyEntry(KEY_ALIAS, privateKey, KEYSTORE_PASSWORD, certs)
        }
    }
}
