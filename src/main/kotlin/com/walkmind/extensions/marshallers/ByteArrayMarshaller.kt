package com.walkmind.extensions.marshallers

interface ByteArrayMarshaller<T> {
    fun encode(value: T): ByteArray
    fun decode(value: ByteArray): T
}