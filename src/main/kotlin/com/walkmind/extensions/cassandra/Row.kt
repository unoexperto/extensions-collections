package com.walkmind.extensions.cassandra

import com.walkmind.extensions.serializers.ByteBufSerializer
import io.netty.buffer.ByteBuf

// https://pandaforme.gitbooks.io/introduction-to-cassandra/content/understand_the_cassandra_data_model.html

// Making row thumbstone will force next merge() to act as put()
// merge() will exclude columns whose timestamp is older than timestamp of thumbstone row!
data class Row(val columns: List<Cell>, val localDeletionTime: Int = Int.MAX_VALUE, val markedForDeleteAt: Long = Long.MIN_VALUE) {

    companion object {
        val serializer = object : ByteBufSerializer<Row> {

            // https://github.com/facebook/rocksdb/blob/v6.5.3/utilities/cassandra/format.cc#L233
            override fun encode(value: Row, out: ByteBuf) {
                out.writeInt(value.localDeletionTime)
                out.writeLong(value.markedForDeleteAt)
                value.columns.forEach { cell ->
                    Cell.serializer.encode(cell, out)
                }
            }

            override fun decode(input: ByteBuf): Row {
                val localDeletionTime = input.readInt()
                val markedForDeleteAt = input.readLong()
                val items = mutableListOf<Cell>()
                while (input.readableBytes() > 0) {
                    val item = Cell.serializer.decode(input)
                    items.add(item)
                }

                return Row(items, localDeletionTime, markedForDeleteAt)
            }

            override val isBounded: Boolean = true
            override val name: String = "Row"
        }
    }

    fun isTombstone(): Boolean {
        return markedForDeleteAt > Long.MIN_VALUE
    }
}