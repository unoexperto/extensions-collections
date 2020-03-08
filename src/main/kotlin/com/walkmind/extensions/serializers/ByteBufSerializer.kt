package com.walkmind.extensions.serializers

import com.walkmind.extensions.misc.ObjectPool
import com.walkmind.extensions.misc.use
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled
import javax.crypto.Cipher

inline fun <R> ByteBuf.use(block: (ByteBuf) -> R): R {
    try {
        return block(this)
    } catch (e: Throwable) {
        throw e
    } finally {
        this.release()
    }
}

interface ByteBufSerializer<T> {
    fun encode(value: T, out: ByteBuf)
    fun decode(input: ByteBuf): T

    @JvmDefault
    fun <V> bimap(enc: (V) -> T, dec: (T) -> V): ByteBufSerializer<V> = object : ByteBufSerializer<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufSerializer.encode(enc(value), out)
        }

        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufSerializer.decode(input))
        }
    }

    @JvmDefault
    fun encodeToArray(value: T): ByteArray {
        val buf = PooledByteBufAllocator.DEFAULT.heapBuffer()
        this@ByteBufSerializer.encode(value, buf)
        val res = ByteArray(buf.readableBytes())
        buf.readBytes(res)
        return res
    }

    @JvmDefault
    fun toByteArraySerializer(): ByteArraySerializer<T> = object : ByteArraySerializer<T> {
        override fun encode(value: T): ByteArray {
            val buf = PooledByteBufAllocator.DEFAULT.heapBuffer()
            this@ByteBufSerializer.encode(value, buf)
            val res = ByteArray(buf.readableBytes())
            buf.readBytes(res)
            return res
        }

        override fun decode(value: ByteArray): T {
            val buf = Unpooled.wrappedBuffer(value)
            return this@ByteBufSerializer.decode(buf)
        }
    }

    companion object {
        @JvmField
        val intSerializer = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeInt(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readInt()
            }
        }

        @JvmField
        val longSerializer = object : ByteBufSerializer<Long> {
            override fun encode(value: Long, out: ByteBuf) {
                out.writeLong(value)
            }

            override fun decode(input: ByteBuf): Long {
                return input.readLong()
            }
        }

        @JvmField
        val utf8Serializer = object : ByteBufSerializer<String> {
            override fun encode(value: String, out: ByteBuf) {
                out.writeCharSequence(value, Charsets.UTF_8)
            }

            override fun decode(input: ByteBuf): String {
                return input.readCharSequence(input.readableBytes(), Charsets.UTF_8).toString()
            }
        }

        @JvmField
        val byteArraySerializer = object : ByteBufSerializer<ByteArray> {
            override fun encode(value: ByteArray, out: ByteBuf) {
                out.writeInt(value.size)
                out.writeBytes(value)
            }

            override fun decode(input: ByteBuf): ByteArray {
                val size = input.readInt()
                val result = ByteArray(size)
                input.readBytes(result)
                return result
            }
        }

        @JvmField
        val utf8SizedSerializer = object : ByteBufSerializer<String> {
            override fun encode(value: String, out: ByteBuf) {
                out.writeInt(ByteBufUtil.utf8Bytes(value))
                out.writeCharSequence(value, Charsets.UTF_8)
            }

            override fun decode(input: ByteBuf): String {
                return input.readCharSequence(input.readInt(), Charsets.UTF_8).toString()
            }
        }

        @JvmStatic
        fun <T> listSerializer(serializer: ByteBufSerializer<T>): ByteBufSerializer<List<T>> {
            return object : ByteBufSerializer<List<T>> {
                override fun encode(value: List<T>, out: ByteBuf) {
                    out.writeInt(value.size)
                    for (item in value)
                        serializer.encode(item, out)
                }

                override fun decode(input: ByteBuf): List<T> {
                    val size = input.readInt()
                    val res = ArrayList<T>(size)
                    for (i in 0 until size)
                        res.add(serializer.decode(input))
                    return res
                }
            }
        }

        @JvmStatic
        fun <K, V> mapSerializer(km: ByteBufSerializer<K>, vm: ByteBufSerializer<V>): ByteBufSerializer<Map<K, V>> {
            return object : ByteBufSerializer<Map<K, V>> {
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

        @JvmStatic
        fun <T> encrypted(
                serializer: ByteBufSerializer<T>,
                encodePool: ObjectPool<Cipher>,
                decodePool: ObjectPool<Cipher>): ByteBufSerializer<T> {

            return object : ByteBufSerializer<T> {
                override fun encode(value: T, out: ByteBuf) {

                    assert(out.hasArray())
                    PooledByteBufAllocator.DEFAULT.heapBuffer().use { raw ->
                        encodePool.use { cipher ->
                            serializer.encode(value, raw)
                            val rawSize = raw.readableBytes()
                            val sizeEncoded = cipher.getOutputSize(rawSize)

                            out.ensureWritable(sizeEncoded + 4)

                            out.writeInt(sizeEncoded)
                            val written = cipher.doFinal(
                                    raw.array(), raw.arrayOffset() + raw.readerIndex(), rawSize,
                                    out.array(), out.arrayOffset() + out.writerIndex())

                            out.writerIndex(out.writerIndex() + written)
                        }
                    }
                }

                override fun decode(input: ByteBuf): T {
                    assert(input.hasArray())
                    return decodePool.use { cipher ->
                        val encryptedSize = input.readInt()
                        val decodedSize = cipher.getOutputSize(encryptedSize)

                        PooledByteBufAllocator.DEFAULT.heapBuffer(decodedSize).use { raw ->

                            val written = cipher.doFinal(
                                    input.array(), input.arrayOffset() + input.readerIndex(), encryptedSize,
                                    raw.array(), raw.arrayOffset() + raw.writerIndex())
                            input.readerIndex(input.readerIndex() + encryptedSize)
                            raw.writerIndex(raw.writerIndex() + written)

                            serializer.decode(raw)
                        }
                    }
                }
            }
        }
    }
}


