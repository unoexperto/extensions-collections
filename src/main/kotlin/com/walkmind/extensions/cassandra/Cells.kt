package com.walkmind.extensions.cassandra

import com.walkmind.extensions.serializers.ByteBufSerializer
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

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

interface Cell {
    companion object {
        val serializer = object : ByteBufSerializer<Cell> {

            // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L73
            // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L108
            // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L173
            override fun encode(value: Cell, out: ByteBuf) {

                when (value) {
                    is RegularCell -> {
                        out.writeByte(CellFlag.Regular.value)
                        out.writeByte(value.index.toInt())
                        out.writeLong(value.timestamp)
                        out.writeInt(value.payload.size)
                        out.writeBytes(value.payload)
                    }
                    is ExpiringCell -> {
                        out.writeByte(CellFlag.Expiring.value)
                        out.writeByte(value.index.toInt())
                        out.writeLong(value.timestamp)
                        out.writeInt(value.payload.size)
                        out.writeBytes(value.payload)
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
            override fun decode(input: ByteBuf): Cell {
                val flags: Int = input.readByte().toInt()
                if (flags == CellFlag.Regular.value) {
                    val index = input.readByte()
                    val timestamp = input.readLong()
                    val payload = ByteArray(input.readInt())
                    input.readBytes(Unpooled.wrappedBuffer(payload).resetWriterIndex(), payload.size)
                    return RegularCell(index, timestamp, payload)
                } else
                    if ((flags and CellFlag.Expiring.value) > 0) {
                        val index = input.readByte()
                        val timestamp = input.readLong()
                        val payload = ByteArray(input.readInt())
                        input.readBytes(Unpooled.wrappedBuffer(payload).resetWriterIndex(), payload.size)
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

            override val isBounded: Boolean = true
            override val name: String = "Cell"
        }
    }

    val index: Byte
}

data class RegularCell(
        override val index: Byte,
        val timestamp: Long,
        val payload: ByteArray) : Cell

data class ExpiringCell(
        override val index: Byte,
        val timestamp: Long,
        val payload: ByteArray,
        val ttl: Int) : Cell

data class TombstoneCell(
        override val index: Byte,
        val localDeletionTime: Int, // seconds
        val markedForDeleteAt: Long) : Cell