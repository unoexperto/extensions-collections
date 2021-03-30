import com.walkmind.extensions.cassandra.RegularCell
import com.walkmind.extensions.cassandra.Row
import com.walkmind.extensions.cassandra.RowCodec
import com.walkmind.extensions.cassandra.TombstoneCell
import com.walkmind.extensions.serializers.ByteBufSerializer
import com.walkmind.extensions.serializers.use
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.rocksdb.*
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.file.Files

class RocksDBTest {

    companion object {
        @JvmStatic
        @BeforeAll
        fun initialization() {
            RocksDB.loadLibrary()
        }
    }

    @Test
    fun operatorOption() {
        StringAppendOperator().use { stringAppendOperator ->
            Options()
                .setCreateIfMissing(true)
                .setMergeOperator(stringAppendOperator).use { opt ->
                    val dbFile = Files.createTempDirectory("ROCKSDB_").toFile().absolutePath

                    RocksDB.open(opt, dbFile).use { db ->
                        // Writing aa under key
                        db.put("key".toByteArray(), "aa".toByteArray())
                        // Writing bb under key
                        db.merge("key".toByteArray(), "bb".toByteArray())
                        val value = db["key".toByteArray()]
                        val strValue = String(value)
                        Assertions.assertEquals(strValue, "aa,bb")
                    }

                    RocksDB.destroyDB(dbFile, opt)
                }
        }
    }

    @Test
    @Throws(InterruptedException::class, RocksDBException::class)
    fun uint64AddOperatorOption() {

        UInt64AddOperator().use { uint64AddOperator ->
            Options()
                .setCreateIfMissing(true)
                .setMergeOperator(uint64AddOperator)
                .use { opt ->

                    val dbFile = Files.createTempDirectory("ROCKSDB_").toFile().absolutePath

                    RocksDB.open(opt, dbFile).use { db ->
                        // Writing (long)100 under key
                        db.put("key".toByteArray(), longToByteArray(100))
                        // Writing (long)1 under key
                        db.merge("key".toByteArray(), longToByteArray(1))
                        val value = db["key".toByteArray()]
                        val longValue: Long = longFromByteArray(value)
                        Assertions.assertEquals(longValue, 101)
                    }
                }
        }
    }

    private fun longToByteArray(l: Long): ByteArray? {
        val buf: ByteBuffer = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE)
        buf.putLong(l)
        return buf.array()
    }

    private fun longFromByteArray(a: ByteArray): Long {
        val buf: ByteBuffer = ByteBuffer.allocate(java.lang.Long.SIZE / java.lang.Byte.SIZE)
        buf.put(a)
        buf.flip()
        return buf.getLong()
    }

    @Test
    fun cassandraOperatorOption() {

        // Grace period is only used to remove Thumbstone columns if (local_deleted_at + gc_grace_period < now())
        PooledByteBufAllocator.DEFAULT.heapBuffer().use { buf2 ->
            CassandraValueMergeOperator(1).use { operator ->
                Options()
                    .setCreateIfMissing(true)
                    .setMergeOperator(operator).use { opt ->
                        val dbFile = Files.createTempDirectory("ROCKSDB_").toFile().absolutePath
                        println(dbFile)

                        RocksDB.open(opt, dbFile).use { db ->

                            val keyBytes = "key".toByteArray()

                            val cell1 = RegularCell(Byte.MIN_VALUE, 1, "john doe")
                            val cell2 = RegularCell(Byte.MAX_VALUE, 9, "god")

                            val cell3 = RegularCell(Byte.MAX_VALUE, 12, "carlos")
//                            val cellThumb = TombstoneCell(0, (System.currentTimeMillis() / 1000).toInt(), System.currentTimeMillis())

                            val row1 = Row(listOf(cell1, cell2))
                            val row2 = Row(listOf(cell3))

                            println(255.toUByte().toByte())

                            val codec = RowCodec(ByteBufSerializer.utf8)

                            db.put(keyBytes, codec.encode(row1))
                            val value1 = codec.decode(db.get(keyBytes))
                            println("Value 1:")
                            for (cell in value1.columns)
                                println("${cell.index}: ${(cell as RegularCell).payload}")

//                            Thread.currentThread().join(2000)
                            db.merge(keyBytes, codec.encode(row2))
                            val value2 = codec.decode(db.get(keyBytes))

                            println("\nValue 2:")
                            for (cell in value2.columns)
                                println("${cell.index}: ${(cell as RegularCell).payload}")

                            Assertions.assertTrue(true)
                        }

                        RocksDB.destroyDB(dbFile, opt)
                    }
            }
        }
    }
}
