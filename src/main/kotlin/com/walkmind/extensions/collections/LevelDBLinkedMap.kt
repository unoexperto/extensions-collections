package com.walkmind.extensions.collections

import com.walkmind.extensions.marshallers.ByteArrayMarshaller
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.*
import java.io.Closeable
import java.io.File
import java.util.logging.Logger
import kotlin.math.min

class DefaultLevelDBComparator : DBComparator {

    override fun name(): String {
        return "leveldb.BytewiseComparator"
    }

    override fun compare(o1: ByteArray?, o2: ByteArray?): Int {
        val a1 = (o1 ?: return -1)
        val a2 = (o2 ?: return 1)

        if (a1.size != a2.size)
            return a1.size - a2.size // https://github.com/google/leveldb/blob/master/include/leveldb/slice.h#L101-L111
        else {
            for (index in 0 until a1.size)
                if (a1[index] != a2[index])
                    return (0xff and a1[index].toInt()) - (0xff and a2[index].toInt())

            return 0;
        }
//        return UnsignedBytes.lexicographicalComparator().compare(o1, o2)
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
        return key;
    }
}

class LevelDBLinkedMap<K, V>(path: File,
                             comparator: DBComparator?,
                             private val keyMarshaller: ByteArrayMarshaller<K>,
                             private val valueMarshaller: ByteArrayMarshaller<V>,
                             blockSize: Int = 64 * 1024,
                             private val deleteBatchSize: Int = 5*1024,
                             private val compressOnExit: Boolean = false) : LinkedMap<K, V>, Closeable {

    companion object {
        private val LOG = Logger.getLogger(LevelDBLinkedMap::class.java.name)
    }

    private val db: DB
    private val readOptions = ReadOptions().verifyChecksums(true).fillCache(false)
    private val writeOptions = WriteOptions().sync(true)

    init {
        var options =
                Options()
                        .blockSize(blockSize)
                        .createIfMissing(true)
                        .compressionType(CompressionType.SNAPPY)

        if (comparator != null)
            options = options.comparator(comparator)

        db = JniDBFactory.factory.open(path, options)
        LOG.finer("Opened DB from ${path.absolutePath}")
    }

    override fun get(key: K): V? {
        return db.get(keyMarshaller.encode(key), readOptions)?.let { valueMarshaller.decode(it) }
    }

    override fun isEmpty(): Boolean {
        db.iterator(readOptions).use {
            it.seekToFirst()
            return it.hasNext()
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
        db.iterator(readOptions).use {
            val byteFrom = keyMarshaller.encode(keyFrom)
            val byteTo = keyMarshaller.encode(keyTo)
            it.seek(byteFrom)

            while (it.hasNext())
                db.createWriteBatch().use { batch ->
                    var count = 0
                    while (count < deleteBatchSize && it.hasNext()) {
                        val byteKey = it.next().key!!
                        if (byteKey.contentEquals(byteTo)) {
                            batch.delete(byteKey)
                            db.write(batch, writeOptions)
                            return
                        }
                        batch.delete(byteKey)
                        count++
                    }
                    db.write(batch, writeOptions)
                }

            db.compactRange(byteFrom, byteTo)
        }
    }

    override fun put(key: K, value: V) {
        db.put(keyMarshaller.encode(key), valueMarshaller.encode(value), writeOptions)
    }

    override fun putAll(from: Iterable<Pair<K, V>>) {
        db.createWriteBatch().use { batch ->
            for ((key, value) in from)
                batch.put(keyMarshaller.encode(key), valueMarshaller.encode(value))
            db.write(batch, writeOptions)
        }
    }

    override fun firstKey(): K? {
        db.iterator(readOptions).use {
            it.seekToFirst()
            return it.runCatching { keyMarshaller.decode(peekNext().key) }.getOrNull()
        }
    }

    override fun lastKey(): K? {
        db.iterator(readOptions).use {
            it.seekToLast()
            return it.runCatching { keyMarshaller.decode(peekNext().key) }.getOrNull()
        }
    }

    override fun iterator(): CloseablePeekingIterator<Pair<K, V>> {
        return object : CloseablePeekingIterator<Pair<K, V>> {
            private var bytesRead = 0L

            private val iter = db.iterator(readOptions)

            init {
                iter.seekToFirst()
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
                    Pair(keyMarshaller.decode(k), valueMarshaller.decode(v))
                }
            }

            override fun close() {
                iter.close()
            }

            override fun peek(): Pair<K, V> {
                return iter.peekNext().let { (k, v) -> Pair(keyMarshaller.decode(k), valueMarshaller.decode(v)) }
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
                    return Pair(keyMarshaller.decode(k), valueMarshaller.decode(v))
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
                    db.compactRange(keyMarshaller.encode(first), keyMarshaller.encode(last))
                }
            }
            LOG.finer("Completed final compaction")
        }
        db.close()
    }

    override fun remove(key: K) {
        db.delete(keyMarshaller.encode(key), writeOptions)
    }

    fun getProperty(name: String): String? {
        return db.getProperty(name)
    }
}
