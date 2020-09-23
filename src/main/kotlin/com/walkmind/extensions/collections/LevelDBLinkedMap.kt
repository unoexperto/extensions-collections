package com.walkmind.extensions.collections

import com.walkmind.extensions.serializers.ByteArraySerializer
import com.walkmind.extensions.misc.compareBytes
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.*
import java.io.Closeable
import java.io.File
import java.util.logging.Logger
import kotlin.math.min

class DefaultLevelDBComparator : DBComparator {

    companion object {
        val INSTANCE = DefaultLevelDBComparator()
    }

    override fun name(): String {
        return "leveldb.BytewiseComparator"
    }

    override fun compare(o1: ByteArray?, o2: ByteArray?): Int {
        return compareBytes(o1, o2)
    }

    override fun findShortestSeparator(startIn: ByteArray?, limitIn: ByteArray?): ByteArray? {
        val start = startIn ?: return startIn
        val limit = limitIn ?: return startIn

        // Find length of common prefix
        val minLength = min(start.size, limit.size)
        var sharedBytes = 0
        while (sharedBytes < minLength && start[sharedBytes] == limit[sharedBytes])
            sharedBytes++

        if (sharedBytes < minLength) {
            val lastSharedByte = start[sharedBytes]
            if (lastSharedByte < 0xff && lastSharedByte + 1 < limit[sharedBytes]) {
                val shortest = ByteArray(sharedBytes + 1)
                System.arraycopy(start, 0, shortest, 0, sharedBytes + 1)
                shortest[sharedBytes]++
                assert(compare(shortest, limit) < 0)
                return shortest
            }
        }

        // Do not shorten if one string is a prefix of the other
        return start
    }

    override fun findShortSuccessor(input: ByteArray?): ByteArray? {

        val key = input ?: return input

        // Find first character that can be incremented
        for (i in 0 until key.size) {
            if (key[i] != 0xff.toByte()) {
                val shortSuccessor = ByteArray(i + 1)
                System.arraycopy(key, 0, shortSuccessor, 0, shortSuccessor.size)
                shortSuccessor[i]++
                return shortSuccessor
            }
        }

        // *key is a run of 0xffs.  Leave it alone.
        return key
    }
}

class LevelDBLinkedMap<K, V>(private val path: File,
                             comparator: DBComparator?,
                             private val keySerializer: ByteArraySerializer<K>,
                             private val valueSerializer: ByteArraySerializer<V>,
                             blockSize: Int = 64 * 1024,
                             private val deleteBatchSize: Int = 5 * 1024,
                             private val compressOnExit: Boolean = false) : LinkedMap<K, V>, MapBatchWriter<K, V>, Closeable, Destroyable {

    private class LevelDBWriteBatch<K, V>(private val p: LevelDBLinkedMap<K, V>) : MapWriteBatch<K, V> {

        private val batch = p.db.createWriteBatch()

        override fun put(key: K, value: V): Int {
            val valueByteArray = p.valueSerializer.encode(value)
            batch.put(p.keySerializer.encode(key), valueByteArray)
            return valueByteArray.size
        }

        override fun merge(key: K, value: V) {
            throw UnsupportedOperationException("Level DB doesn't support merge() in batches")
        }

        override fun remove(key: K) {
            batch.delete(p.keySerializer.encode(key))
        }

        override fun clear() {
            throw UnsupportedOperationException("Level DB doesn't support clear() in batches")
        }

        override fun close() {
            batch.close()
        }

        override fun commit() {
            p.db.write(batch, p.writeOptions)
        }
    }

    companion object {
        private val LOG = Logger.getLogger(LevelDBLinkedMap::class.java.name)
    }

    private val db: DB
    private val readOptions = ReadOptions().verifyChecksums(true).fillCache(false)
    private val writeOptions = WriteOptions().sync(true)
    private val initOptions: Options = Options()
            .blockSize(blockSize)
            .createIfMissing(true)
            .compressionType(CompressionType.SNAPPY)
            .comparator(comparator)

    init {
        db = JniDBFactory.factory.open(path, initOptions)
        LOG.finer("Opened DB from ${path.absolutePath}")
    }

    override fun get(key: K): V? {
        return db.get(keySerializer.encode(key), readOptions)?.let { valueSerializer.decode(it) }
    }

    override fun isEmpty(): Boolean {
        db.iterator(readOptions).use {
            it.seekToFirst()
            return !it.hasNext()
        }
    }

    override fun clear() {
        db.iterator(readOptions).use {
            it.seekToFirst()
            while (it.hasNext())
                db.delete(it.next().key)
        }
    }

    override fun removeRange(keyFrom: K, keyTo: K) {
        val byteFrom = keySerializer.encode(keyFrom)
        val byteTo = keySerializer.encode(keyTo)

        if (DefaultLevelDBComparator.INSTANCE.compare(byteFrom, byteTo) < 0)
            db.iterator(readOptions).use {
                it.seek(byteFrom)

                while (it.hasNext())
                    db.createWriteBatch().use { batch ->
                        var count = 0

                        while (count < deleteBatchSize && it.hasNext()) {
                            val byteKey = it.next().key!!
                            if (DefaultLevelDBComparator.INSTANCE.compare(byteKey, byteTo) < 0) {
                                batch.delete(byteKey)
                                count++
                            } else {
                                if (count > 0)
                                    db.write(batch, writeOptions)
                                return
                            }
                        }
                        db.write(batch, writeOptions)
                    }

                db.compactRange(byteFrom, byteTo)
            }
    }

    override fun put(key: K, value: V): Int {
        val valueByteArray = valueSerializer.encode(value)
        db.put(keySerializer.encode(key), valueByteArray, writeOptions)
        return valueByteArray.size
    }

    override fun putAll(from: Iterable<Pair<K, V>>) {
        db.createWriteBatch().use { batch ->
            for ((key, value) in from)
                batch.put(keySerializer.encode(key), valueSerializer.encode(value))
            db.write(batch, writeOptions)
        }
    }

    override fun merge(key: K, value: V) {
        throw UnsupportedOperationException("Level DB doesn't support merge()")
    }

    override fun mergeAll(from: Iterable<Pair<K, V>>) {
        throw UnsupportedOperationException("Level DB doesn't support mergeAll()")
    }

    override fun firstKey(): K? {
        db.iterator(readOptions).use {
            it.seekToFirst()
            return it.runCatching { keySerializer.decode(peekNext().key) }.getOrNull()
        }
    }

    override fun lastKey(): K? {
        db.iterator(readOptions).use {
            it.seekToLast()
            return it.runCatching { keySerializer.decode(peekNext().key) }.getOrNull()
        }
    }

    override fun lastKey(start: K): K? {
        throw UnsupportedOperationException()
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

            private val iter = db.iterator(readOptions)

            init {
                if (start == null)
                    iter.seekToFirst()
                else
                    iter.seek(keySerializer.encode(start))
            }

            override fun hasNext(): Boolean {

                val res = iter.hasNext()
                if (!res) {
                    LOG.finest("$bytesRead bytes delivered by iterator")
                }
                return res
            }

            override fun next(): Pair<K, V> {
                return iter.next().let { (k, v) ->
                    bytesRead += v.size
                    Pair(keySerializer.decode(k), valueSerializer.decode(v))
                }
            }

            override fun close() {
                iter.close()
            }

            override fun peek(): Pair<K, V> {
                return iter.peekNext().let { (k, v) -> Pair(keySerializer.decode(k), valueSerializer.decode(v)) }
            }
        }
    }

    /*
    override fun iterator(): CloseablePeekingIterator<Pair<K, V>> {
        return object : CloseablePeekingIterator<Pair<K, V>> {
            private var bytesRead = 0L

            private val iter = db.iterator(readOptions)
            private var peeked: Pair<K, V>? = null

            init {
                iter.seekToFirst()
            }

            override fun hasNext(): Boolean {
                if (peeked != null)
                    return true
                else {
                    val res = iter.hasNext()
                    if (!res) {
                        LOG.finest("$bytesRead bytes delivered by iterator")
                    }
                    return res
                }
            }

            override fun next(): Pair<K, V> {
                if (peeked != null) {
                    val peeked1 = peeked!!
                    peeked = null
                    return peeked1
                } else {
                    return nextReal() ?: throw NoSuchElementException()
                }
            }

            override fun close() {
                iter.close()
            }

            override fun peek(): Pair<K, V> {
                if (peeked == null)
                    peeked = nextReal()

                return peeked ?: throw NoSuchElementException()
            }

            private fun nextReal(): Pair<K, V>? {
                if (iter.hasNext()) {
                    val (k, v) = iter.next()
                    bytesRead += v.size
                    return Pair(keySerializer.decode(k), valueSerializer.decode(v))
                } else
                    return null
            }
        }
    }
    */

    override fun close() {
        if (compressOnExit) {
            LOG.finer("Begin final compaction")
            firstKey()?.let { first ->
                lastKey()?.let { last ->
                    db.compactRange(keySerializer.encode(first), keySerializer.encode(last))
                }
            }
            LOG.finer("Completed final compaction")
        }
        db.close()
    }

    override fun remove(key: K) {
        db.delete(keySerializer.encode(key), writeOptions)
    }

    fun getProperty(name: String): String? {
        return db.getProperty(name)
    }

    override fun destroy() {
        JniDBFactory.factory.destroy(path, initOptions)
    }

    override fun newWriteBatch(): MapWriteBatch<K, V> {
        return LevelDBWriteBatch(this)
    }
}
