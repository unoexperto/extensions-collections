package com.walkmind.extensions.collections

interface SimpleSequence<T> {
    fun addAll(elements: Collection<T>)
    fun clear()
    fun isEmpty(): Boolean
    fun add(element: T)
    fun peek(): T? // returns head and does not remove it, null if empty
    fun pop(): T?  // returns head and removes it, null if empty
}

interface PeekingIterator<out T> {
    // Returns the next element in the iteration and switch to next element.
    operator fun next(): T

    // Returns the next element in the iteration without traversing further.
    fun peek(): T

    // Returns `true` if the iteration has more elements.
    operator fun hasNext(): Boolean
}

interface CloseablePeekingIterator<out T> : PeekingIterator<T>, AutoCloseable

interface LinkedMap<K, V> {
    fun get(key: K): V?
    fun isEmpty(): Boolean
    fun clear()
    fun put(key: K, value: V)
    fun putAll(from: Iterable<Pair<K, V>>)
    fun merge(key: K, value: V)
    fun mergeAll(from: Iterable<Pair<K, V>>)
    fun remove(key: K)
    fun removeRange(keyFrom: K, keyTo: K)
    fun firstKey(): K?
    fun lastKey(): K?

    // Existing iterators should work after modification of the map
    fun iterator(): CloseablePeekingIterator<Pair<K, V>>
    fun iterator(prefix: K): CloseablePeekingIterator<Pair<K, V>>
}

interface DestroyableStorage {
    fun destroy()
}