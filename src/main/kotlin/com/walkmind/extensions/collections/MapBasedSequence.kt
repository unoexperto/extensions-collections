package com.walkmind.extensions.collections

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class MapBasedSequence<T> constructor(private val map: LinkedMap<Long, T>, private val iteratorModeDistance: Int, private val discardThreshold: Int) : SimpleSequence<T>, Iterator<T>, AutoCloseable {

    private val lock = ReentrantLock()
    private var nextWriteSeqNo: Long

    private var readerIt: CloseablePeekingIterator<Pair<Long, T>>
    private var nextReadSeqNo: Long = 1
    private var useIterator: Boolean = false

    private var firstReadSeqNo: Long

    private var dirty: Boolean = false

    init {
        require(iteratorModeDistance > 0) { "iteratorModeDistance must be positive" }
        require(discardThreshold > 0) { "discardThreshold must be positive" }

        readerIt = map.iterator()
        nextReadSeqNo = map.firstKey() ?: 1L
        firstReadSeqNo = nextReadSeqNo
        nextWriteSeqNo = map.lastKey()?.plus(1) ?: 1L
        trySwitchingToIterMode()
    }

    private fun refreshReaderIterator() {
        if (dirty && !readerIt.hasNext()) {
            nextReadSeqNo = map.firstKey() ?: 1L
            readerIt.close()
            readerIt = map.iterator()
            dirty = false
        }
    }

    override fun addAll(elements: Collection<T>) {
        lock.withLock {
            map.putAll(elements.mapIndexed { index, t ->
                Pair(nextWriteSeqNo + index, t)
            })
            nextWriteSeqNo += elements.size
            dirty = true
            trySwitchingToIterMode()
        }
    }

    override fun clear() {
        lock.withLock {
            map.clear()

            nextReadSeqNo = 1
            nextWriteSeqNo = 1
            dirty = true
            useIterator = false;
        }
    }

    override fun isEmpty(): Boolean {
        lock.withLock {
            if (useIterator) {
                refreshReaderIterator()
                return !readerIt.hasNext()
            } else
                return nextReadSeqNo == nextWriteSeqNo
        }
    }

    override fun add(element: T) {
        lock.withLock {
            map.put(nextWriteSeqNo++, element)
            dirty = true
            trySwitchingToIterMode()
        }
    }

    override fun peek(): T? {
        lock.withLock {

            if (nextReadSeqNo < nextWriteSeqNo) {
                if (useIterator) {
                    refreshReaderIterator()

                    return if (readerIt.hasNext())
                        readerIt.peek().second
                    else
                        null
                } else
                    return map.get(nextReadSeqNo)
            } else
                return null
        }
    }

    override fun pop(): T? {
        lock.withLock {

            if (nextReadSeqNo < nextWriteSeqNo) {
                val result = if (useIterator) {
                    refreshReaderIterator()

                    if (readerIt.hasNext()) {
                        val (key, value) = readerIt.next()
                        assert(nextReadSeqNo == key)
                        nextReadSeqNo++

                        // This iterator is finished but perhaps there is more data
                        if (!readerIt.hasNext())
                            dirty = true

                        value
                    } else
                        null
                } else {
                    val value = map.get(nextReadSeqNo)
                    value
                }

                tryDisablingIterMode()
                discardReadItems()
                return result
            } else
                return null
        }
    }

    // Disable iterator mode if sequence is empty
    private fun tryDisablingIterMode() {
        if (useIterator && nextReadSeqNo == nextWriteSeqNo)
            useIterator = false
    }

    // Switch to iterator when there is more than 'iteratorModeDistance' items in sequence
    private fun trySwitchingToIterMode() {
        if (!useIterator && nextWriteSeqNo - nextReadSeqNo > iteratorModeDistance)
            useIterator = true
    }

    private fun discardReadItems(ignoreThreshold: Boolean = false) {
        if (nextReadSeqNo - firstReadSeqNo > (if (ignoreThreshold) 0 else discardThreshold)) {
            map.removeRange(firstReadSeqNo, nextReadSeqNo)
            firstReadSeqNo = nextReadSeqNo + 1
        }
    }

    override fun close() {
        readerIt.close()
        discardReadItems(true)
    }

    override fun hasNext(): Boolean {
        return !isEmpty()
    }

    override fun next(): T {
        return pop()!!
    }
}
