package com.walkmind.extensions.serializers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

interface ByteArraySerializer<T> {
    fun encode(value: T): ByteArray
    fun decode(value: ByteArray): T

    @JvmDefault
    fun <V>bimap(enc: (V) -> T, dec: (T) -> V): ByteArraySerializer<V> = object : ByteArraySerializer<V> {
        override fun encode(value: V): ByteArray {
            return this@ByteArraySerializer.encode(enc(value))
        }

        override fun decode(value: ByteArray): V {
            return dec(this@ByteArraySerializer.decode(value))
        }
    }
}

object DefaultLongSerializer : ByteArraySerializer<Long> {
    override fun encode(value: Long): ByteArray {
        return ByteBuffer.allocate(Long.SIZE_BYTES).putLong(value).array()
    }

    override fun decode(value: ByteArray): Long {
        return ByteBuffer.allocate(Long.SIZE_BYTES).put(value).flip().long
    }
}

object DefaultStringSerializer : ByteArraySerializer<String> {
    override fun encode(value: String): ByteArray {
        return value.toByteArray(StandardCharsets.UTF_8)
    }

    override fun decode(value: ByteArray): String {
        return String(value, StandardCharsets.UTF_8)
    }
}