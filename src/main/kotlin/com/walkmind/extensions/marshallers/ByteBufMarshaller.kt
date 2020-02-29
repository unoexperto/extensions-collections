package com.walkmind.extensions.marshallers

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled

inline fun <R> ByteBuf.use(block: (ByteBuf) -> R): R {
    try {
        return block(this)
    } catch (e: Throwable) {
        throw e
    } finally {
        this.release()
    }
}

interface ByteBufMarshaller<T> {
    fun encode(value: T, out: ByteBuf)
    fun decode(input: ByteBuf): T

    @JvmDefault
    fun <V> bimap(enc: (V) -> T, dec: (T) -> V): ByteBufMarshaller<V> = object : ByteBufMarshaller<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufMarshaller.encode(enc(value), out)
        }

        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufMarshaller.decode(input))
        }
    }

    @JvmDefault
    fun encodeToArray(value: T): ByteArray {
        val buf = PooledByteBufAllocator.DEFAULT.buffer()
        this@ByteBufMarshaller.encode(value, buf)
        val res = ByteArray(buf.readableBytes())
        buf.readBytes(res)
        return res
    }

    @JvmDefault
    fun toByteArrayMarshaller(): ByteArrayMarshaller<T> = object : ByteArrayMarshaller<T> {
        override fun encode(value: T): ByteArray {
            val buf = PooledByteBufAllocator.DEFAULT.buffer()
            this@ByteBufMarshaller.encode(value, buf)
            val res = ByteArray(buf.readableBytes())
            buf.readBytes(res)
            return res
        }

        override fun decode(value: ByteArray): T {
            val buf = Unpooled.wrappedBuffer(value)
            return this@ByteBufMarshaller.decode(buf)
        }
    }

    companion object {
        @JvmField
        val intMarshaller = object : ByteBufMarshaller<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeInt(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readInt()
            }
        }

        @JvmField
        val longMarshaller = object : ByteBufMarshaller<Long> {
            override fun encode(value: Long, out: ByteBuf) {
                out.writeLong(value)
            }

            override fun decode(input: ByteBuf): Long {
                return input.readLong()
            }
        }

        @JvmField
        val utf8Marshaller = object : ByteBufMarshaller<String> {
            override fun encode(value: String, out: ByteBuf) {
                out.writeCharSequence(value, Charsets.UTF_8)
            }

            override fun decode(input: ByteBuf): String {
                return input.readCharSequence(input.readableBytes(), Charsets.UTF_8).toString()
            }
        }

        @JvmField
        val utf8SizedMarshaller = object : ByteBufMarshaller<String> {
            override fun encode(value: String, out: ByteBuf) {
                out.writeInt(ByteBufUtil.utf8Bytes(value))
                out.writeCharSequence(value, Charsets.UTF_8)
            }

            override fun decode(input: ByteBuf): String {
                return input.readCharSequence(input.readInt(), Charsets.UTF_8).toString()
            }
        }

        @JvmStatic
        fun <T> listMarshaller(marshaller: ByteBufMarshaller<T>): ByteBufMarshaller<ArrayList<T>> {
            return object : ByteBufMarshaller<ArrayList<T>> {
                override fun encode(value: ArrayList<T>, out: ByteBuf) {
                    out.writeInt(value.size)
                    for (item in value)
                        marshaller.encode(item, out)
                }

                override fun decode(input: ByteBuf): ArrayList<T> {
                    val size = input.readInt()
                    val res = ArrayList<T>(size)
                    for (i in 0 until size)
                        res[i] = marshaller.decode(input)
                    return res
                }
            }
        }

        @JvmStatic
        fun <K, V> mapMarshaller(km: ByteBufMarshaller<K>, vm: ByteBufMarshaller<V>): ByteBufMarshaller<Map<K, V>> {
            return object : ByteBufMarshaller<Map<K, V>> {
                override fun encode(value: Map<K, V>, out: ByteBuf) {
                    out.writeInt(value.size)
                    for (pair in value.entries) {
                        km.encode(pair.key, out)
                        vm.encode(pair.value, out)
                    }
                }

                override fun decode(input: ByteBuf): Map<K, V> {
                    val size = input.readInt()
                    val res = mutableMapOf<K, V>()
                    for (i in 0 until size) {
                        val key = km.decode(input)
                        val value = vm.decode(input)
                        res[key] = value
                    }
                    return res
                }
            }
        }
    }
}


