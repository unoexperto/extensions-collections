package com.walkmind.extensions.misc

fun compareBytes(o1: ByteArray?, o2: ByteArray?): Int {
    val a1 = (o1 ?: return -1)
    val a2 = (o2 ?: return 1)

    if (a1.size != a2.size)
        return a1.size - a2.size // https://github.com/google/leveldb/blob/master/include/leveldb/slice.h#L101-L111
    else {
        for (index in 0 until a1.size)
            if (a1[index] != a2[index])
                return (0xff and a1[index].toInt()) - (0xff and a2[index].toInt())

        return 0
    }
//    return UnsignedBytes.lexicographicalComparator().compare(o1, o2)
}

fun ByteArray.startsWith(prefix: ByteArray): Boolean {
    if (this.size < prefix.size)
        return false;

    for (index in 0 until prefix.size)
        if (this[index] != prefix[index])
            return false

    return true
}
