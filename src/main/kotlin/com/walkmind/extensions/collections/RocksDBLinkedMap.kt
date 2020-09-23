package com.walkmind.extensions.collections

import com.walkmind.extensions.serializers.ByteBufSerializer
import com.walkmind.extensions.serializers.use
import io.netty.buffer.PooledByteBufAllocator.DEFAULT
import io.netty.buffer.Unpooled
import org.rocksdb.*
import org.rocksdb.util.SizeUnit
import java.io.Closeable
import java.io.File
import java.util.logging.Logger

//private fun RocksIterator.asIterator(isForward: Boolean): Iterator<Pair<ByteArray, ByteArray>> {
//    val that = this
//    return object : Iterator<Pair<ByteArray, ByteArray>> {
//        override fun hasNext(): Boolean {
//            return that.isValid
//        }
//
//        override fun next(): Pair<ByteArray, ByteArray> {
//            val result = Pair(that.key(), that.value())
//            if (isForward)
//                that.next()
//            else
//                that.prev()
//
//            return result
//        }
//
//    }
//}


class RocksDBLinkedMap<K, V>(
        private val path: File,
        options: Options?,
        syncOnWrite: Boolean,
        private val keySerializer: ByteBufSerializer<K>,
        private val valueSerializer: ByteBufSerializer<V>) : LinkedMap<K, V>, MapBatchWriter<K, V>, Closeable, Destroyable {

//    fun createColumnOptions(): ColumnFamilyOptions? {
//        val blockCacheSize = 268_435_456L
//        val blockSize = 131_072L
//        val targetFileSize = 268_435_456L
//        val writeBufferSize = 67_108_864L
//        return ColumnFamilyOptions()
//                .setCompactionStyle(CompactionStyle.LEVEL)
//                .setLevelCompactionDynamicLevelBytes(true)
//                .setTargetFileSizeBase(targetFileSize)
//                .setMaxBytesForLevelBase(1073741824L)
//                .setWriteBufferSize(writeBufferSize)
//                .setMinWriteBufferNumberToMerge(3)
//                .setMaxWriteBufferNumber(4)
//                .setTableFormatConfig(BlockBasedTableConfig()
//                        .setBlockCache(ClockCache(blockCacheSize))
//                        .setBlockSize(blockSize)
//                        .setFilterPolicy(BloomFilter()))
//    }

    private class RocksDBWriteBatch<K, V>(private val p: RocksDBLinkedMap<K, V>) : MapWriteBatch<K, V> {

        private val batch = WriteBatch()

        override fun put(key: K, value: V): Int {
            val valueByteArray = p.valueSerializer.encode(value)
            batch.put(p.keySerializer.encode(key), valueByteArray)
            return valueByteArray.size
        }

        override fun merge(key: K, value: V) {
            batch.merge(p.keySerializer.encode(key), p.valueSerializer.encode(value))
        }

        override fun remove(key: K) {
            batch.delete(p.keySerializer.encode(key))
        }

        override fun clear() {
            batch.clear()
        }

        override fun close() {
            batch.close()
        }

        override fun commit() {
            p.db.write(p.writeOptions, batch)
        }
    }

    companion object {
        private val LOG = Logger.getLogger(RocksDBLinkedMap::class.java.name)

        private val compressionLevels = arrayListOf(
                CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION
        )

        val defaultOptions = Options()
                .optimizeLevelStyleCompaction()
//                .setUseFsync(false) // rafik
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setNumLevels(compressionLevels.size)
//                .setCompactionPriority()
//                .setMaxBackgroundCompactions(20)
                .setCompressionPerLevel(compressionLevels)
                .setMaxLogFileSize(64 * SizeUnit.MB)
//                .setWriteBufferSize(512 * SizeUnit.MB) // default is 64M
//                .setMinWriteBufferNumberToMerge(2)
//                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCreateIfMissing(true)
                .setInfoLogLevel(InfoLogLevel.ERROR_LEVEL)
//                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
//                .setBloomLocality(1)
//                .setMemtablePrefixBloomSizeRatio(0.1)
                .setMaxOpenFiles(-1)
                .setIncreaseParallelism(4)
//                .setParanoidChecks(true)
//                .setAllowMmapWrites(true)
//                .setAllowMmapReads(true)
                .setUseDirectIoForFlushAndCompaction(true)
                .setUseDirectReads(true)

//                .setStatsDumpPeriodSec(5 * 60)
//                .setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery)
//                .setStatistics(
//                        Statistics().also { it.setStatsLevel(StatsLevel.ALL) }
//                )
    }

    private val db: RocksDB
    private val writeOptions: WriteOptions = WriteOptions().setSync(syncOnWrite)//.setDisableWAL(true)
    private val readOptions = ReadOptions()
    //    private val readOptionsTailing = ReadOptions().setTailing(true)
    private val initOptions: Options = options ?: defaultOptions

    init {
        db = RocksDB.open(initOptions, path.absolutePath)
        LOG.finer("Opened DB from ${path.absolutePath}")
    }

    override fun get(key: K): V? {
        DEFAULT.heapBuffer().use { buf ->
            assert(buf.isContiguous)
            keySerializer.encode(key, buf)
            return db.get(readOptions, buf.array(), buf.arrayOffset(), buf.readableBytes())?.let { bytes ->
                Unpooled.wrappedBuffer(bytes).use(valueSerializer::decode)
            }
        }
    }

    override fun isEmpty(): Boolean {
        db.newIterator().use {
            it.seekToFirst()
            return !it.isValid
        }
    }

    override fun clear() {
        val first = db.newIterator().use {
            it.seekToFirst()
            if (it.isValid) it.key() else null
        }

        val last = db.newIterator().use {
            it.seekToLast()
            if (it.isValid) it.key() else null
        }

        if (first != null && last != null) {
            db.deleteRange(writeOptions, first, last)
            db.delete(writeOptions, last)
        }
    }

    override fun put(key: K, value: V): Int {
        DEFAULT.heapBuffer().use { bufK ->
            assert(bufK.isContiguous)
            keySerializer.encode(key, bufK)

            DEFAULT.heapBuffer().use { bufV ->
                assert(bufV.isContiguous)
                valueSerializer.encode(value, bufV)

                val valueReadableBytes = bufV.readableBytes()
                db.put(writeOptions,
                        bufK.array(), bufK.arrayOffset(), bufK.readableBytes(),
                        bufV.array(), bufV.arrayOffset(), valueReadableBytes)

                return valueReadableBytes
            }
        }
    }

    override fun putAll(from: Iterable<Pair<K, V>>) {

        WriteBatch().use { batch ->
            for ((key, value) in from)
                batch.put(keySerializer.encode(key), valueSerializer.encode(value))

            db.write(writeOptions, batch)
        }
    }

    override fun merge(key: K, value: V) {
        DEFAULT.heapBuffer().use { bufK ->
            assert(bufK.isContiguous)
            keySerializer.encode(key, bufK)

            DEFAULT.heapBuffer().use { bufV ->
                assert(bufV.isContiguous)
                valueSerializer.encode(value, bufV)

                db.merge(writeOptions,
                        bufK.array(), bufK.arrayOffset(), bufK.readableBytes(),
                        bufV.array(), bufV.arrayOffset(), bufV.readableBytes())
            }
        }
    }

    override fun mergeAll(from: Iterable<Pair<K, V>>) {

        WriteBatch().use { batch ->
            for ((key, value) in from)
                batch.merge(keySerializer.encode(key), valueSerializer.encode(value))

            db.write(writeOptions, batch)
        }
    }

    override fun remove(key: K) {
        DEFAULT.heapBuffer().use { buf ->
            assert(buf.isContiguous)
            keySerializer.encode(key, buf)

            db.delete(writeOptions, buf.array(), buf.arrayOffset(), buf.readableBytes())
        }
    }

    override fun removeRange(keyFrom: K, keyTo: K) {
        db.deleteRange(writeOptions, keySerializer.encode(keyFrom), keySerializer.encode(keyTo))
    }

    override fun firstKey(): K? {
        db.newIterator(readOptions).use {
            it.seekToFirst()

            return if (it.isValid)
                Unpooled.wrappedBuffer(it.key()).use(keySerializer::decode)
            else
                null
        }
    }

    override fun lastKey(): K? {
        db.newIterator(readOptions).use {
            it.seekToLast()

            return if (it.isValid)
                Unpooled.wrappedBuffer(it.key()).use(keySerializer::decode)
            else
                null
        }
    }

    override fun lastKey(start: K): K? {
        db.newIterator(readOptions).use {
            it.seekForPrev(keySerializer.encode(start));

            return if (it.isValid)
                Unpooled.wrappedBuffer(it.key()).use(keySerializer::decode)
            else
                null
        }
    }

    fun compact() {
        db.compactRange()
    }

    override fun iterator(): CloseablePeekingIterator<Pair<K, V>> {
        return iteratorInternal(null)
    }

    override fun iterator(prefix: K): CloseablePeekingIterator<Pair<K, V>> {
        return iteratorInternal(prefix)
    }

    private fun iteratorInternal(start: K?): CloseablePeekingIterator<Pair<K, V>> {
        return object : CloseablePeekingIterator<Pair<K, V>> {
            private var bytesRead = 0L

            private val it = db.newIterator(readOptions)

            init {
                if (start == null)
                    it.seekToFirst()
                else
                    it.seek(keySerializer.encode(start))
            }

            override fun hasNext(): Boolean {

                val res = it.isValid
                if (!res) {
                    LOG.finest("$bytesRead bytes delivered by iterator")
                }
                return res
            }

            override fun next(): Pair<K, V> {
                val k = it.key()
                val v = it.value()
                bytesRead += v.size

                val res = Pair(
                        Unpooled.wrappedBuffer(k).use(keySerializer::decode),
                        Unpooled.wrappedBuffer(v).use(valueSerializer::decode)
                )
                it.next()
                return res
            }

            override fun close() {
                it.close()
            }

            override fun peek(): Pair<K, V> {
                return Pair(
                        Unpooled.wrappedBuffer(it.key()).use(keySerializer::decode),
                        Unpooled.wrappedBuffer(it.value()).use(valueSerializer::decode)
                )
            }
        }
    }

    fun createCheckpoint(path: String) {
        Checkpoint.create(db).use {
            it.createCheckpoint(path)
        }
    }

    override fun close() {
        db.close()
    }

    override fun destroy() {
        RocksDB.destroyDB(path.absolutePath, initOptions)
    }

    override fun newWriteBatch(): MapWriteBatch<K, V> {
        return RocksDBWriteBatch(this)
    }
}