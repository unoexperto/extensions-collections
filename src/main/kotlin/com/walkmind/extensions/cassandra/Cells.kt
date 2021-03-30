package com.walkmind.extensions.cassandra

import com.walkmind.extensions.serializers.ByteBufSerializer
import io.netty.buffer.ByteBuf

// https://github.com/apache/cassandra/blob/cassandra-3.11.5/src/java/org/apache/cassandra/db/rows/Cell.java#L178
// https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.h#L69
enum class CellFlag(val value: Int) {
    Regular(0),
    Deleting(1),
    Expiring(2),
    HasEmptyValue(4),
    HasRowTimestampMask(8),
    UseRowTtlMask(0x10)
}

interface Cell<T> {
    val index: Byte
}

data class RegularCell<T>(override val index: Byte, val timestamp: Long, val payload: T) : Cell<T>

data class ExpiringCell<T>(override val index: Byte, val timestamp: Long, val payload: T, val ttl: Int) : Cell<T>

// localDeletionTime is in seconds
data class TombstoneCell<T>(override val index: Byte, val localDeletionTime: Int, val markedForDeleteAt: Long) : Cell<T>

////

class CellCodec<T>(codec: ByteBufSerializer<T>) : ByteBufSerializer<Cell<T>> {

    private val sizedCodec = ByteBufSerializer.sized(ByteBufSerializer.int32, codec)

    override val isBounded: Boolean = true
    override val name: String = "Cell"

    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L73
    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L108
    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L173
    override fun encode(value: Cell<T>, out: ByteBuf) {

        when (value) {
            is RegularCell -> {
                out.writeByte(CellFlag.Regular.value)
                out.writeByte(value.index.toInt())
                out.writeLong(value.timestamp)
                sizedCodec.encode(value.payload, out)
            }
            is ExpiringCell -> {
                out.writeByte(CellFlag.Expiring.value)
                out.writeByte(value.index.toInt())
                out.writeLong(value.timestamp)
                sizedCodec.encode(value.payload, out)
                out.writeInt(value.ttl)
            }
            is TombstoneCell -> {
                out.writeByte(CellFlag.Deleting.value)
                out.writeByte(value.index.toInt())
                out.writeInt(value.localDeletionTime)
                out.writeLong(value.markedForDeleteAt)
            }
        }
    }

    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L80
    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L43
    override fun decode(input: ByteBuf): Cell<T> {
        val flags: Int = input.readByte().toInt()
        if (flags == CellFlag.Regular.value) {
            val index = input.readByte()
            val timestamp = input.readLong()
            val payload = sizedCodec.decode(input)
            return RegularCell(index, timestamp, payload)
        } else
            if ((flags and CellFlag.Expiring.value) > 0) {
                val index = input.readByte()
                val timestamp = input.readLong()
                val payload = sizedCodec.decode(input)
                return ExpiringCell(index, timestamp, payload, input.readInt())
            } else
                if ((flags and CellFlag.Deleting.value) > 0) {
                    val index = input.readByte()
                    val localDeletionTime = input.readInt()
                    val markedForDeleteAt = input.readLong()
                    return TombstoneCell(index, localDeletionTime, markedForDeleteAt)
                } else
                    throw RuntimeException("Unknown Cassandra value flag: $flags")
    }
}
