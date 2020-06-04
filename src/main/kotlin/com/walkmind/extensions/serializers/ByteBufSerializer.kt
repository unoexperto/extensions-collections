@file:Suppress("unused")

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

interface ByteBufEncoder<in T> {
    fun encode(value: T, out: ByteBuf)

    @JvmDefault
    fun encode(value: T): ByteArray {
        PooledByteBufAllocator.DEFAULT.heapBuffer().use { buf ->
            this@ByteBufEncoder.encode(value, buf)
            val res = ByteArray(buf.readableBytes())
            buf.readBytes(res)
            return res
        }
    }

    @JvmDefault
    fun <V> mapEncoder(enc: (V) -> T): ByteBufEncoder<V> = object : ByteBufEncoder<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufEncoder.encode(enc(value), out)
        }
    }
}

interface ByteBufDecoder<out T> {
    fun decode(input: ByteBuf): T

    @JvmDefault
    fun decode(input: ByteArray): T {
        return this@ByteBufDecoder.decode(Unpooled.wrappedBuffer(input))
    }

    @JvmDefault
    fun <V> mapDecoder(dec: (T) -> V): ByteBufDecoder<V> = object : ByteBufDecoder<V> {
        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufDecoder.decode(input))
        }
    }
}

interface ByteBufSerializer<T> : ByteBufEncoder<T>, ByteBufDecoder<T> {

    @JvmDefault
    fun <V> bimap(enc: (V) -> T, dec: (T) -> V): ByteBufSerializer<V> = object : ByteBufSerializer<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufSerializer.encode(enc(value), out)
        }

        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufSerializer.decode(input))
        }
    }

//    @JvmDefault
//    fun toByteArraySerializer(): ByteArraySerializer<T> = object : ByteArraySerializer<T> {
//        override fun encode(value: T): ByteArray {
//            PooledByteBufAllocator.DEFAULT.heapBuffer().use { buf ->
//                this@ByteBufSerializer.encode(value, buf)
//                val res = ByteArray(buf.readableBytes())
//                buf.readBytes(res)
//                return res
//            }
//        }
//
//        override fun decode(value: ByteArray): T {
//            val buf = Unpooled.wrappedBuffer(value)
//            return this@ByteBufSerializer.decode(buf)
//        }
//    }

    companion object {
        @JvmField
        val boolSerializer = object : ByteBufSerializer<Boolean> {
            override fun encode(value: Boolean, out: ByteBuf) {
                out.writeBoolean(value)
            }

            override fun decode(input: ByteBuf): Boolean {
                return input.readBoolean()
            }
        }

        @JvmField
        val byteSerializer = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeByte(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readByte().toInt()
            }
        }

        @JvmField
        val shortSerializer = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeShort(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readShort().toInt()
            }
        }

        @JvmField
        val shortSerializerLE = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeShortLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readShortLE().toInt()
            }
        }

        @JvmField
        val mediumSerializer = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeMedium(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readMedium()
            }
        }

        @JvmField
        val mediumSerializerLE = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeMediumLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readMediumLE()
            }
        }

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
        val intSerializerLE = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeIntLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readIntLE()
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
        val longSerializerLE = object : ByteBufSerializer<Long> {
            override fun encode(value: Long, out: ByteBuf) {
                out.writeLongLE(value)
            }

            override fun decode(input: ByteBuf): Long {
                return input.readLongLE()
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
        val utf8SizedSerializer = object : ByteBufSerializer<String> {
            override fun encode(value: String, out: ByteBuf) {
                out.writeInt(ByteBufUtil.utf8Bytes(value))
                out.writeCharSequence(value, Charsets.UTF_8)
            }

            override fun decode(input: ByteBuf): String {
                return input.readCharSequence(input.readInt(), Charsets.UTF_8).toString()
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

        @JvmStatic
        fun <T> listSerializer(sizeSer: ByteBufSerializer<Int>, itemSer: ByteBufSerializer<T>): ByteBufSerializer<List<T>> {
            return object : ByteBufSerializer<List<T>> {
                override fun encode(value: List<T>, out: ByteBuf) {
                    sizeSer.encode(value.size, out)
                    for (item in value)
                        itemSer.encode(item, out)
                }

                override fun decode(input: ByteBuf): List<T> {
                    val size = sizeSer.decode(input)
                    val res = ArrayList<T>(size)
                    for (i in 0 until size)
                        res.add(itemSer.decode(input))
                    return res
                }
            }
        }

        @JvmStatic
        fun <K, V> mapSerializer(sizeSer: ByteBufSerializer<Int>, km: ByteBufSerializer<K>, vm: ByteBufSerializer<V>): ByteBufSerializer<Map<K, V>> {
            return object : ByteBufSerializer<Map<K, V>> {
                override fun encode(value: Map<K, V>, out: ByteBuf) {
                    sizeSer.encode(value.size, out)
                    for (pair in value.entries) {
                        km.encode(pair.key, out)
                        vm.encode(pair.value, out)
                    }
                }

                override fun decode(input: ByteBuf): Map<K, V> {
                    val size = sizeSer.decode(input)
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
        fun <T> nullable(serializer: ByteBufSerializer<T>): ByteBufSerializer<T?> {
            return object : ByteBufSerializer<T?> {
                override fun encode(value: T?, out: ByteBuf) {
                    if (value == null)
                        out.writeBoolean(false)
                    else {
                        out.writeBoolean(true)
                        serializer.encode(value, out)
                    }
                }

                override fun decode(input: ByteBuf): T? {
                    if (input.readBoolean())
                        return serializer.decode(input)
                    else
                        return null
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

        @JvmStatic
        fun <T> sized(sizeSer: ByteBufSerializer<Int>, itemSer: ByteBufSerializer<T>): ByteBufSerializer<T> {
            return object : ByteBufSerializer<T> {
                override fun decode(input: ByteBuf): T {
                    val size = sizeSer.decode(input)
                    val readValue = itemSer.decode(input.slice(input.readerIndex(), size))
                    input.readerIndex(input.readerIndex() + size)
                    return readValue
                }

                override fun encode(value: T, out: ByteBuf) {
                    val sizeIndex = out.writerIndex()
                    sizeSer.encode(0, out)
                    val afterSizeIndex = out.writerIndex()
                    itemSer.encode(value, out)
                    val endIndex = out.writerIndex()
                    out.writerIndex(sizeIndex)
                    sizeSer.encode(endIndex - afterSizeIndex, out)
                    out.writerIndex(endIndex)
                }
            }
        }
    }
}


