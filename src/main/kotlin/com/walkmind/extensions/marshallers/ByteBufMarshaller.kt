package com.walkmind.extensions.marshallers

import io.netty.buffer.ByteBuf

interface ByteBufMarshaller<T> {
    fun encode(value: T, out: ByteBuf)
    fun decode(input: ByteBuf): T
}