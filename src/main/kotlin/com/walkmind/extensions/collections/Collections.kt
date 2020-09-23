package com.walkmind.extensions.collections

interface SimpleSequence<T> {
    fun addAll(elements: Collection<T>)
    fun clear()
    fun isEmpty(): Boolean
    fun add(element: T)
    fun peek(): T? // returns head and does not remove it, null if empty
    fun pop(): T?  // returns head and removes it, null if empty
}

interface PeekingIterator<out T> : Iterator<T> {
    // Returns the next element in the iteration without traversing further.
    fun peek(): T
}

interface CloseableIterator<out T> : Iterator<T>, AutoCloseable

interface CloseablePeekingIterator<out T> : PeekingIterator<T>, AutoCloseable

interface MapWriteOps<K, V> {
    fun put(key: K, value: V): Int
    fun merge(key: K, value: V)
    fun remove(key: K)
    fun clear()
}

interface LinkedMap<K, V> : MapWriteOps<K, V> {
    fun get(key: K): V?
    fun isEmpty(): Boolean
    fun putAll(from: Iterable<Pair<K, V>>)
    fun mergeAll(from: Iterable<Pair<K, V>>)
    fun removeRange(keyFrom: K, keyTo: K)
    fun firstKey(): K?
    fun lastKey(): K?
    fun lastKey(start: K): K?

    // Existing iterators should work after modification of the map
    fun iterator(): CloseablePeekingIterator<Pair<K, V>>

    fun iterator(prefix: K): CloseablePeekingIterator<Pair<K, V>>
}

interface MapWriteBatch<K, V> : MapWriteOps<K, V>, AutoCloseable {
    fun commit()
}

interface MapBatchWriter<K, V> {
    fun newWriteBatch(): MapWriteBatch<K, V>
}

interface Destroyable {
    fun destroy()
}
