package com.here.naksha.handler.http

import com.here.naksha.lib.core.EventHandlerParams
import java.net.MalformedURLException
import java.net.URL
import java.util.*
import java.util.concurrent.TimeUnit

/** The parameter parser. */
class HttpHandlerParams(connectorParams: Map<String, Any?>) : EventHandlerParams() {

    /** Enums */
    companion object {
        const val URL = "url"
        const val CONN_TIMEOUT = "connTimeout"
        const val READ_TIMEOUT = "readTimeout"
        const val HTTP_METHOD = "httpMethod"
        const val GZIP = "gzip"
        const val HTTP_GET = "GET"
        const val HTTP_PUT = "PUT"
        const val HTTP_POST = "POST"
        const val HTTP_PATCH = "PATCH"
    }

    /** The URL to which to POST the events. */
    val url: URL

    /** The timeout in milliseconds when trying to establish a new connection to the `url`. */
    val connTimeout: Long

    /** Abort read timeout in milliseconds. */
    val readTimeout: Long

    /** HTTP method */
    val httpMethod: String

    /**
     * If the JSON should be gzip compressed, `null` means auto-detect, `true` means always compress,
     * `false` means never compress.
     */
    val gzip: Boolean?

    /**
     * Parse the given connector params into this type-safe class.
     *
     * @param connectorParams the connector parameters.
     * @throws NullPointerException if a value is `null` that must not be null.
     * @throws IllegalArgumentException if a value has an invalid type, for example a map expected,
     *     and a string found.
     * @throws MalformedURLException if the given URL is invalid.
     */
    init {
        url = URL(parseValue(connectorParams, URL, String::class.java))
        connTimeout = parseValueWithDefault(
            connectorParams,
            CONN_TIMEOUT,
            TimeUnit.SECONDS.toMillis(5)
        )
        readTimeout = parseValueWithDefault(
            connectorParams,
            READ_TIMEOUT,
            TimeUnit.SECONDS.toMillis(60)
        )
        httpMethod = parseValueWithDefault(connectorParams, HTTP_METHOD, HTTP_POST)
        gzip = parseOptionalValue(connectorParams, GZIP, Boolean::class.java)
    }
}
