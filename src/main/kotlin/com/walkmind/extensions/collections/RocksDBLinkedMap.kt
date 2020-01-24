package com.walkmind.extensions.collections

import com.walkmind.extensions.marshallers.ByteArrayMarshaller
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
        private val keyMarshaller: ByteArrayMarshaller<K>,
        private val valueMarshaller: ByteArrayMarshaller<V>) : LinkedMap<K, V>, Closeable, DestroyableStorage {

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
//                .setUseFsync(false) // rafik
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setNumLevels(compressionLevels.size)
//            .setCompactionPriority()
                .setMaxBackgroundCompactions(20)
                .setCompressionPerLevel(compressionLevels)
                .setMaxLogFileSize(64 * SizeUnit.MB)
//            .setWriteBufferSize(512 * SizeUnit.MB) // default is 64M
//            .setMinWriteBufferNumberToMerge(2)

//            .setCompactionStyle(CompactionStyle.LEVEL)
                .setCreateIfMissing(true)
                .setInfoLogLevel(InfoLogLevel.ERROR_LEVEL)
//            .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                //.setBloomLocality(1)
                //.setMemtablePrefixBloomSizeRatio(0.1)
                .setMaxOpenFiles(-1)
                .setIncreaseParallelism(4)
//            .setParanoidChecks(true)
                //.setAllowMmapWrites(true)
                //.setAllowMmapReads(true)
                .setUseDirectIoForFlushAndCompaction(true)
                .setUseDirectReads(true)
        //.setStatsDumpPeriodSec(5 * 60)
//            .setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery)
//                .setStatistics(
//                        Statistics().also { it.setStatsLevel(StatsLevel.ALL) }
//                )
    }

    private val db: RocksDB
    private val writeOptions: WriteOptions = WriteOptions().setSync(false)//.setDisableWAL(true)
    private val readOptions = ReadOptions()
//    private val readOptionsTailing = ReadOptions().setTailing(true)
    private val initOptions: Options = options ?: defaultOptions

    init {
        db = RocksDB.open(initOptions, path.absolutePath)
        LOG.finer("Opened DB from ${path.absolutePath}")
    }

    override fun get(key: K): V? {
        return db.get(readOptions, keyMarshaller.encode(key))?.let { valueMarshaller.decode(it) }
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

    override fun put(key: K, value: V) {
        db.put(writeOptions, keyMarshaller.encode(key), valueMarshaller.encode(value))
    }

    override fun putAll(from: Iterable<Pair<K, V>>) {

        WriteBatch().use { batch ->
            for ((key, value) in from)
                batch.put(keyMarshaller.encode(key), valueMarshaller.encode(value))

            db.write(writeOptions, batch)
        }
    }

    override fun merge(key: K, value: V) {
        db.merge(writeOptions, keyMarshaller.encode(key), valueMarshaller.encode(value))
    }

    override fun mergeAll(from: Iterable<Pair<K, V>>) {

        WriteBatch().use { batch ->
            for ((key, value) in from)
                batch.merge(keyMarshaller.encode(key), valueMarshaller.encode(value))

            db.write(writeOptions, batch)
        }
    }

    override fun remove(key: K) {
        db.delete(writeOptions, keyMarshaller.encode(key))
    }

    override fun removeRange(keyFrom: K, keyTo: K) {
        db.deleteRange(writeOptions, keyMarshaller.encode(keyFrom), keyMarshaller.encode(keyTo))
    }

    override fun firstKey(): K? {
        db.newIterator(readOptions).use {
            it.seekToFirst()

            return if (it.isValid)
                keyMarshaller.decode(it.key())
            else
                null
        }
    }

    override fun lastKey(): K? {
        db.newIterator(readOptions).use {
            it.seekToLast()

            return if (it.isValid)
                keyMarshaller.decode(it.key())
            else
                null
        }
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
                    it.seek(keyMarshaller.encode(start))
            }

            override fun hasNext(): Boolean {

                val res = it.isValid()
                if (!res) {
                    LOG.finest("$bytesRead bytes delivered by iterator")
                }
                return res
            }

            override fun next(): Pair<K, V> {
                val k = it.key()
                val v = it.value()
                bytesRead += v.size

                val res = Pair(keyMarshaller.decode(k), valueMarshaller.decode(v))
                it.next()
                return res
            }

            override fun close() {
                it.close()
            }

            override fun peek(): Pair<K, V> {
                return Pair(keyMarshaller.decode(it.key()), valueMarshaller.decode(it.value()))
            }
        }
    }

    override fun close() {
        db.close()
    }

    override fun destroy() {
        RocksDB.destroyDB(path.absolutePath, initOptions)
    }
}