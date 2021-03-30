package com.walkmind.extensions.cassandra

import com.walkmind.extensions.serializers.ByteBufSerializer
import io.netty.buffer.ByteBuf

// https://pandaforme.gitbooks.io/introduction-to-cassandra/content/understand_the_cassandra_data_model.html

// Making row thumbstone will force next merge() to act as put()
// merge() will exclude columns whose timestamp is older than timestamp of thumbstone row!
data class Row<T>(
    val columns: List<Cell<T>>,
    val localDeletionTime: Int = Int.MAX_VALUE,
    val markedForDeleteAt: Long = Long.MIN_VALUE
) {
    val isTombstone: Boolean
        get() {
            return markedForDeleteAt > Long.MIN_VALUE
        }
}

////

class RowCodec<T>(codec: ByteBufSerializer<T>) : ByteBufSerializer<Row<T>> {

    private val cellCodec = CellCodec(codec)

    override val isBounded: Boolean = false
    override val name: String = "Row"

    // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L233
    override fun encode(value: Row<T>, out: ByteBuf) {
        out.writeInt(value.localDeletionTime)
        out.writeLong(value.markedForDeleteAt)
        for (cell in value.columns)
            cellCodec.encode(cell, out)
    }

    override fun decode(input: ByteBuf): Row<T> {
        val localDeletionTime = input.readInt()
        val markedForDeleteAt = input.readLong()
        val items = mutableListOf<Cell<T>>()
        while (input.readableBytes() > 0) {
            val item = cellCodec.decode(input)
            items.add(item)
        }

        return Row(items, localDeletionTime, markedForDeleteAt)
    }
}
