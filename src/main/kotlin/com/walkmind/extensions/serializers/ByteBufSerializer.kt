@file:Suppress("unused")

package com.walkmind.extensions.serializers

import com.walkmind.extensions.misc.ObjectPool
import com.walkmind.extensions.misc.use
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.Charset
import java.time.*
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
    val isBounded: Boolean
    val name: String

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
    fun <V> mapEncoder(name: String, enc: (V) -> T): ByteBufEncoder<V> = object : ByteBufEncoder<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufEncoder.encode(enc(value), out)
        }

        override val isBounded: Boolean = this@ByteBufEncoder.isBounded
        override val name: String = name
    }
}

interface ByteBufDecoder<out T> {
    fun decode(input: ByteBuf): T
    val isBounded: Boolean
    val name: String

    @JvmDefault
    fun decode(input: ByteArray): T {
        return this@ByteBufDecoder.decode(Unpooled.wrappedBuffer(input))
    }

    @JvmDefault
    fun <V> mapDecoder(name: String, dec: (T) -> V): ByteBufDecoder<V> = object : ByteBufDecoder<V> {
        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufDecoder.decode(input))
        }

        override val isBounded: Boolean = this@ByteBufDecoder.isBounded
        override val name: String = name
    }
}

interface ByteBufSerializer<T> : ByteBufEncoder<T>, ByteBufDecoder<T> {

    @JvmDefault
    fun <V> bimap(name: String, enc: (V) -> T, dec: (T) -> V): ByteBufSerializer<V> = object : ByteBufSerializer<V> {
        override fun encode(value: V, out: ByteBuf) {
            return this@ByteBufSerializer.encode(enc(value), out)
        }

        override fun decode(input: ByteBuf): V {
            return dec(this@ByteBufSerializer.decode(input))
        }

        override val isBounded: Boolean = this@ByteBufSerializer.isBounded
        override val name: String = name
    }

    companion object {
        @JvmField
        val bool = object : ByteBufSerializer<Boolean> {
            override fun encode(value: Boolean, out: ByteBuf) {
                out.writeBoolean(value)
            }

            override fun decode(input: ByteBuf): Boolean {
                return input.readBoolean()
            }

            override val isBounded = true
            override val name: String = "Bool"
        }

        @JvmField
        val byte1 = object : ByteBufSerializer<Byte> {
            override fun encode(value: Byte, out: ByteBuf) {
                out.writeByte(value.toInt())
            }

            override fun decode(input: ByteBuf): Byte {
                return input.readByte()
            }

            override val isBounded = true
            override val name: String = "Byte"
        }

        @JvmField
        val short16 = object : ByteBufSerializer<Short> {
            override fun encode(value: Short, out: ByteBuf) {
                out.writeShort(value.toInt())
            }

            override fun decode(input: ByteBuf): Short {
                return input.readShort()
            }

            override val isBounded = true
            override val name: String = "Short16"
        }

        @JvmField
        val short16L = object : ByteBufSerializer<Short> {
            override fun encode(value: Short, out: ByteBuf) {
                out.writeShortLE(value.toInt())
            }

            override fun decode(input: ByteBuf): Short {
                return input.readShortLE()
            }

            override val isBounded = true
            override val name: String = "Short16L"
        }

        @JvmField
        val int8 = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeByte(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readByte().toInt()
            }

            override val isBounded = true
            override val name: String = "Int8"
        }

        @JvmField
        val int16 = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeShort(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readShort().toInt()
            }

            override val isBounded = true
            override val name: String = "Int16"
        }

        @JvmField
        val int16L = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeShortLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readShortLE().toInt()
            }

            override val isBounded = true
            override val name: String = "Int16L"
        }

        @JvmField
        val int24 = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeMedium(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readMedium()
            }

            override val isBounded = true
            override val name: String = "Int24"
        }

        @JvmField
        val int24L = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeMediumLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readMediumLE()
            }

            override val isBounded = true
            override val name: String = "Int24L"
        }

        @JvmField
        val int32 = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeInt(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readInt()
            }

            override val isBounded = true
            override val name: String = "Int32"
        }

        @JvmField
        val int32L = object : ByteBufSerializer<Int> {
            override fun encode(value: Int, out: ByteBuf) {
                out.writeIntLE(value)
            }

            override fun decode(input: ByteBuf): Int {
                return input.readIntLE()
            }

            override val isBounded = true
            override val name: String = "Int32L"
        }

        @JvmField
        val long64 = object : ByteBufSerializer<Long> {
            override fun encode(value: Long, out: ByteBuf) {
                out.writeLong(value)
            }

            override fun decode(input: ByteBuf): Long {
                return input.readLong()
            }

            override val isBounded = true
            override val name: String = "Long64"
        }

        @JvmField
        val long64L = object : ByteBufSerializer<Long> {
            override fun encode(value: Long, out: ByteBuf) {
                out.writeLongLE(value)
            }

            override fun decode(input: ByteBuf): Long {
                return input.readLongLE()
            }

            override val isBounded = true
            override val name: String = "Long64L"
        }

        @JvmField
        val float32 = object : ByteBufSerializer<Float> {
            override fun encode(value: Float, out: ByteBuf) {
                out.writeFloat(value)
            }

            override fun decode(input: ByteBuf): Float {
                return input.readFloat()
            }

            override val isBounded = true
            override val name: String = "Float"
        }

        @JvmField
        val float32L = object : ByteBufSerializer<Float> {
            override fun encode(value: Float, out: ByteBuf) {
                out.writeFloatLE(value)
            }

            override fun decode(input: ByteBuf): Float {
                return input.readFloatLE()
            }

            override val isBounded = true
            override val name: String = "FloatL"
        }

        @JvmField
        val double64 = object : ByteBufSerializer<Double> {
            override fun encode(value: Double, out: ByteBuf) {
                out.writeDouble(value)
            }

            override fun decode(input: ByteBuf): Double {
                return input.readDouble()
            }

            override val isBounded = true
            override val name: String = "Double"
        }

        @JvmField
        val double64L = object : ByteBufSerializer<Double> {
            override fun encode(value: Double, out: ByteBuf) {
                out.writeDoubleLE(value)
            }

            override fun decode(input: ByteBuf): Double {
                return input.readDoubleLE()
            }

            override val isBounded = true
            override val name: String = "DoubleL"
        }

        @JvmField
        val utf8 = StringSerializer(Charsets.UTF_8)

        @JvmField
        val utf8Sized = SizedUtf8Serializer(int32)

        @JvmField
        val byteArray: ByteBufSerializer<ByteArray> = object : ByteBufSerializer<ByteArray> {
            override fun encode(value: ByteArray, out: ByteBuf) {
                out.writeBytes(value)
            }

            override fun decode(input: ByteBuf): ByteArray {
                val result = ByteArray(input.readableBytes())
                input.readBytes(result)
                return result
            }

            override val isBounded: Boolean = false
            override val name: String = "ByteArray"
        }

        @JvmField
        val byteArraySized = SizedByteArraySerializer(int32)

        @JvmField
        val bigInt = byteArraySized.bimap("BigInt", BigInteger::toByteArray, ::BigInteger)

        private class BigDecimalSerializer : ByteBufSerializer<BigDecimal> {
            override fun encode(value: BigDecimal, out: ByteBuf) {
                bigInt.encode(value.unscaledValue(), out)
                out.writeInt(value.scale())
            }

            override fun decode(input: ByteBuf): BigDecimal {
                return BigDecimal(bigInt.decode(input), input.readInt())
            }

            override val isBounded: Boolean = true
            override val name: String = "BigDecimal"
        }

        @JvmField
        val bigDecimal: ByteBufSerializer<BigDecimal> = BigDecimalSerializer()

        @JvmField
        val instant64 = long64.bimap("Instant64", Instant::toEpochMilli, { millis -> Instant.ofEpochMilli(millis) })

        @JvmField
        val instant64L = long64L.bimap("Instant64L", Instant::toEpochMilli, { millis -> Instant.ofEpochMilli(millis) })

        private class InstantSerializer96(private val long64ser: ByteBufSerializer<Long>, private val int32ser: ByteBufSerializer<Int>) : ByteBufSerializer<Instant> {
            override fun encode(value: Instant, out: ByteBuf) {
                long64ser.encode(value.epochSecond, out)
                int32ser.encode(value.nano, out)
            }

            override fun decode(input: ByteBuf): Instant {
                val epochSecond = long64ser.decode(input)
                val nano = int32ser.decode(input)
                return Instant.ofEpochSecond(epochSecond, nano.toLong())
            }

            override val isBounded: Boolean = true
            override val name: String = "Instant96"
        }

        @JvmField
        val instant96: ByteBufSerializer<Instant> = InstantSerializer96(long64, int32)

        @JvmField
        val instant96L: ByteBufSerializer<Instant> = InstantSerializer96(long64L, int32L)

        private class LocalDateTimeSerializer(private val ser: ByteBufSerializer<Instant>) : ByteBufSerializer<LocalDateTime> {
            private val utc = ZoneId.of("UTC")

            override fun encode(value: LocalDateTime, out: ByteBuf) {
                ser.encode(value.toInstant(ZoneOffset.UTC), out)
            }

            override fun decode(input: ByteBuf): LocalDateTime {
                return LocalDateTime.ofInstant(ser.decode(input), utc)
            }

            override val isBounded: Boolean = ser.isBounded
            override val name: String = "LocalDateTime"
        }

        @JvmField
        val localDateTime64: ByteBufSerializer<LocalDateTime> = LocalDateTimeSerializer(instant64)

        @JvmField
        val localDateTime64L: ByteBufSerializer<LocalDateTime> = LocalDateTimeSerializer(instant64L)

        @JvmField
        val localDateTime96: ByteBufSerializer<LocalDateTime> = LocalDateTimeSerializer(instant96)

        @JvmField
        val localDateTime96L: ByteBufSerializer<LocalDateTime> = LocalDateTimeSerializer(instant96L)

        @JvmField
        val localDate = long64.bimap("LocalDate", LocalDate::toEpochDay, LocalDate::ofEpochDay)

        @JvmField
        val localDateL = long64L.bimap("LocalDateL", LocalDate::toEpochDay, LocalDate::ofEpochDay)

        @JvmStatic
        fun <T> listSerializer(sizeSer: ByteBufSerializer<Int>, itemSer: ByteBufSerializer<T>): ByteBufSerializer<List<T>> {
            return ListSerializer(sizeSer, itemSer)
        }

        @JvmStatic
        fun <T> setSerializer(sizeSer: ByteBufSerializer<Int>, itemSer: ByteBufSerializer<T>): ByteBufSerializer<Set<T>> {
            return SetSerializer(sizeSer, itemSer)
        }

        @JvmStatic
        fun <K, V> mapSerializer(sizeSer: ByteBufSerializer<Int>, km: ByteBufSerializer<K>, vm: ByteBufSerializer<V>): ByteBufSerializer<Map<K, V>> {
            return MapSerializer(sizeSer, km, vm)
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

                override val isBounded: Boolean = serializer.isBounded
                override val name: String = "nullable(${serializer.name})"
            }
        }

        @JvmStatic
        fun <T> encrypted(
                serializer: ByteBufSerializer<T>,
                encodePool: ObjectPool<Cipher>,
                decodePool: ObjectPool<Cipher>): ByteBufSerializer<T> {

            return EncryptedSerializer(serializer, encodePool, decodePool)
        }

        @JvmStatic
        fun <T> sized(sizeSer: ByteBufSerializer<Int>, itemSer: ByteBufSerializer<T>): ByteBufSerializer<T> {
            return SizedSerializer(sizeSer, itemSer)
        }

        @JvmStatic
        fun <T> lazy(ser: ByteBufSerializer<T>): ByteBufSerializer<T> {
            return object : ByteBufSerializer<T> {
                override fun encode(value: T, out: ByteBuf) {
                    TODO("Not yet implemented")
                }

                override fun decode(input: ByteBuf): T {
                    TODO("Not yet implemented")
                }

                override val isBounded: Boolean = ser.isBounded
                override val name: String = ser.name
            }
        }
    }
}

/*
class KnownObjectSerializer: ByteBufSerializer<Any> {
    enum class JType {
        BigDecimal,
        Byte,
        ByteArray,
        Double,
        Float,
        Instant,
        Int,
//        JsonNode,
        List,
        LocalDate,
        LocalDateTime,
        Long,
        Map,
        Set,
        Short,
        String
    }

    val listSer: ByteBufSerializer<List<Any>> by lazy {
        ByteBufSerializer.listSerializer(ByteBufSerializer.int32, this)
    }
    private val setSer: ByteBufSerializer<Set<Any>> by lazy {
        ByteBufSerializer.setSerializer(ByteBufSerializer.int32, this)
    }
    private val mapSer: ByteBufSerializer<Map<String, Any>> by lazy {
        ByteBufSerializer.mapSerializer(ByteBufSerializer.int32, ByteBufSerializer.utf8Sized, this)
    }

    override fun encode(value: Any, out: ByteBuf) {
        when (value) {
            is BigDecimal -> {
                out.writeByte(JType.BigDecimal.ordinal)
                ByteBufSerializer.bigDecimal.encode(value, out)
            }
            is Byte -> {
                out.writeByte(JType.Byte.ordinal)
                ByteBufSerializer.byte1.encode(value, out)
            }
            is ByteArray -> {
                out.writeByte(JType.ByteArray.ordinal)
                ByteBufSerializer.byteArraySized.encode(value, out)
            }
            is Double -> {
                out.writeByte(JType.Double.ordinal)
                ByteBufSerializer.double64.encode(value, out)
            }
            is Float -> {
                out.writeByte(JType.Float.ordinal)
                ByteBufSerializer.float32.encode(value, out)
            }
            is Instant -> {
                out.writeByte(JType.Instant.ordinal)
                ByteBufSerializer.instant.encode(value, out)
            }
            is Int -> {
                out.writeByte(JType.Int.ordinal)
                ByteBufSerializer.int32.encode(value, out)
            }
            is List<*> -> {
                out.writeByte(JType.List.ordinal)
                listSer.encode(value as List<Any>, out)
            }
            is LocalDate -> {
                out.writeByte(JType.LocalDate.ordinal)
                ByteBufSerializer.localDate.encode(value, out)
            }
            is LocalDateTime -> {
                out.writeByte(JType.LocalDateTime.ordinal)
                ByteBufSerializer.localDateTime.encode(value, out)
            }
            is Long -> {
                out.writeByte(JType.Long.ordinal)
                ByteBufSerializer.long64.encode(value, out)
            }
            is Map<*, *> -> {
                out.writeByte(JType.Map.ordinal)
                mapSer.encode(value as Map<String, Any>, out)
            }
            is Set<*> -> {
                out.writeByte(JType.Set.ordinal)
                setSer.encode(value as Set<Any>, out)
            }
            is Short -> {
                out.writeByte(JType.Short.ordinal)
                ByteBufSerializer.short16.encode(value, out)
            }
            is String -> {
                out.writeByte(JType.String.ordinal)
                ByteBufSerializer.utf8Sized.encode(value, out)
            }
            else -> {
                TODO("Not yet implemented")
            }
        }
    }

    override fun decode(input: ByteBuf): Any {
        return when (JType.values()[input.readByte().toInt()]) {
            JType.BigDecimal -> ByteBufSerializer.bigDecimal.decode(input)
            JType.Byte -> ByteBufSerializer.byte1.decode(input)
            JType.ByteArray -> ByteBufSerializer.byteArraySized.decode(input)
            JType.Double -> ByteBufSerializer.double64.decode(input)
            JType.Float -> ByteBufSerializer.float32.decode(input)
            JType.Instant -> ByteBufSerializer.instant.decode(input)
            JType.Int -> ByteBufSerializer.int32.decode(input)
//            JType.JsonNode -> ByteBufSerializer.bigDecimal.decode(input)
            JType.List -> listSer.decode(input)
            JType.LocalDate -> ByteBufSerializer.localDate.decode(input)
            JType.LocalDateTime -> ByteBufSerializer.localDateTime.decode(input)
            JType.Long -> ByteBufSerializer.long64.decode(input)
            JType.Map -> mapSer.decode(input)
            JType.Set -> setSer.decode(input)
            JType.Short -> ByteBufSerializer.short16.decode(input)
            JType.String -> ByteBufSerializer.utf8Sized.decode(input)
            else -> TODO()
        }
    }

    override val isBounded: Boolean by lazy { true }
    override val name: String by lazy { "Any" }
}
*/

class MapSerializer<K, V>(
        private val sizeSer: ByteBufSerializer<Int>,
        private val km: ByteBufSerializer<K>,
        private val vm: ByteBufSerializer<V>) : ByteBufSerializer<Map<K, V>> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
        require(km.isBounded) { "${km.name} is not bounded" }
        require(vm.isBounded) { "${vm.name} is not bounded" }
    }

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

        assert(res.size == size)
        return res
    }

    override val isBounded: Boolean = true
    override val name: String = "Map[${km.name}, ${vm.name}](size: ${sizeSer.name})"
}

class SetSerializer<T>(private val sizeSer: ByteBufSerializer<Int>, private val itemSer: ByteBufSerializer<T>) : ByteBufSerializer<Set<T>> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
        require(itemSer.isBounded) { "${itemSer.name} is not bounded" }
    }

    override fun encode(value: Set<T>, out: ByteBuf) {
        sizeSer.encode(value.size, out)
        for (item in value)
            itemSer.encode(item, out)
    }

    override fun decode(input: ByteBuf): Set<T> {
        val size = sizeSer.decode(input)
        val res = mutableSetOf<T>()
        for (i in 0 until size)
            res.add(itemSer.decode(input))

        assert(res.size == size)
        return res
    }

    override val isBounded = true
    override val name: String = "Set[${itemSer.name}](size: ${sizeSer.name})"
}

class ListSerializer<T>(private val sizeSer: ByteBufSerializer<Int>, private val itemSer: ByteBufSerializer<T>) : ByteBufSerializer<List<T>> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
        require(itemSer.isBounded) { "${itemSer.name} is not bounded" }
    }

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

    override val isBounded = true
    override val name: String = "List[${itemSer.name}](size: ${sizeSer.name})"
}

class StringSerializer(private val charset: Charset) : ByteBufSerializer<String> {
    override fun encode(value: String, out: ByteBuf) {
        out.writeCharSequence(value, charset)
    }

    override fun decode(input: ByteBuf): String {
        return input.readCharSequence(input.readableBytes(), charset).toString()
    }

    override val isBounded = false
    override val name: String = "String(${charset.name()})"
}

class SizedSerializer<T>(private val sizeSer: ByteBufSerializer<Int>, private val itemSer: ByteBufSerializer<T>) : ByteBufSerializer<T> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
    }

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

    override val isBounded = true
    override val name: String = "Sized(${sizeSer.name}, ${itemSer.name})"
}

class SizedUtf8Serializer(private val sizeSer: ByteBufSerializer<Int>) : ByteBufSerializer<String> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
    }

    override fun encode(value: String, out: ByteBuf) {
        sizeSer.encode(ByteBufUtil.utf8Bytes(value), out)
        out.writeCharSequence(value, Charsets.UTF_8)
    }

    override fun decode(input: ByteBuf): String {
        return input.readCharSequence(sizeSer.decode(input), Charsets.UTF_8).toString()
    }

    override val isBounded = true
    override val name: String = "SizedUtf8(${sizeSer.name})"
}

class EncryptedSerializer<T>(
        private val serializer: ByteBufSerializer<T>,
        private val encodePool: ObjectPool<Cipher>,
        private val decodePool: ObjectPool<Cipher>) : ByteBufSerializer<T> {

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

    override val isBounded = true
    override val name: String = "Encrypted(${serializer.name})"
}

class SizedByteArraySerializer(private val sizeSer: ByteBufSerializer<Int>) : ByteBufSerializer<ByteArray> {

    init {
        require(sizeSer.isBounded) { "${sizeSer.name} is not bounded" }
    }

    override fun encode(value: ByteArray, out: ByteBuf) {
        sizeSer.encode(value.size, out)
        out.writeBytes(value)
    }

    override fun decode(input: ByteBuf): ByteArray {
        val size = sizeSer.decode(input)
        val result = ByteArray(size)
        input.readBytes(result)
        return result
    }

    override val isBounded = true
    override val name: String = "SizedByteArray(${sizeSer.name})"
}