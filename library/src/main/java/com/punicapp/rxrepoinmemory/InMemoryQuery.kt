package com.punicapp.rxrepoinmemory

import com.punicapp.rxrepocore.AbstractQuery
import com.punicapp.rxrepocore.Check
import com.punicapp.rxrepocore.LocalFilter
import com.punicapp.rxrepocore.SortDir
import io.reactivex.annotations.Nullable
import java.util.*

internal class InMemoryQuery<out T : Any>(private var collection: MutableCollection<T>?) : AbstractQuery<T>() {

    private val filtered: Collection<T>?
        @Nullable
        get() {
            if (filters.isEmpty() && sorting.isEmpty()) {
                return collection
            }
            for (filter in filters) {
                collection = applyFilter(collection, filter)
            }

            if (sorting.size > 0) {
                val list = ArrayList(collection!!)
                sortList(list)
                collection = list
            }

            return collection
        }

    override fun find(): List<T> {
        val filtered = filtered
        return if (filtered != null && filtered.isNotEmpty()) if (filtered is List<*>) filtered as List<T> else ArrayList(filtered) else emptyList()
    }

    override fun first(): T? {
        val filtered = filtered
        return if (filtered != null && filtered.isNotEmpty()) filtered.iterator().next() else null
    }

    override fun count(): Long {
        val filtered = filtered
        return (filtered?.size ?: 0).toLong()
    }

    override fun remove(): Int {
        val filtered = filtered
        if (filtered != null) {
            collection!!.removeAll(filtered)
            return filtered.size
        }
        return 0
    }

    private fun sortList(list: List<T>?) {
        if (list == null || list.isEmpty()) {
            return
        }

        try {
            for (localSort in sorting) {
                val prop = localSort.property
                val sortDir = localSort.sort

                val obj = list.iterator().next()
                val field = obj.javaClass.getDeclaredField(prop)
                field.isAccessible = true

                Collections.sort(list, Comparator { o1, o2 ->
                    try {
                        val value1 = field.get(o1) as Comparable<Any>
                        val value2 = field.get(o2) as Comparable<Any>
                        var compared = value1.compareTo(value2)
                        if (sortDir === SortDir.Desc) compared = -compared
                        return@Comparator compared
                    } catch (e: IllegalAccessException) {
                        e.printStackTrace()
                        return@Comparator 0
                    }
                })
            }
        } catch (e: NoSuchFieldException) {
            e.printStackTrace()
        }

    }

    private fun applyFilter(us: Collection<T>?, filter: LocalFilter): MutableCollection<T>? {
        if (us == null || us.isEmpty()) {
            return null
        }

        val check = filter.check
        val prop = filter.idProp
        val requestedValue = filter.value

        var retList: MutableList<T>? = null
        try {
            val obj = us.iterator().next()
            val field = obj.javaClass.getDeclaredField(prop)
            field.isAccessible = true


            retList = ArrayList()

            var objList: List<Any>? = null
            if (check === Check.In)
                objList = Arrays.asList(*requestedValue as Array<Any>)
            for (item in us) {
                val value = field.get(item)
                var checkResult = false
                when (check) {
                    Check.Equal -> checkResult = requestedValue == value
                    Check.NotEqual -> checkResult = requestedValue != value
                    Check.GreatOrEqual, Check.LowerOrEqual -> if (value != null) {
                        val compared = (requestedValue as Comparable<Any>).compareTo(value as Comparable<Any>)
                        checkResult = compared <= 0
                    }
                    Check.IsNull -> checkResult = value == null
                    Check.IsNotNull -> checkResult = value != null
                    Check.In -> checkResult = objList!!.contains(value)
                }

                if (checkResult) retList.add(item)
            }
        } catch (e: NoSuchFieldException) {
            e.printStackTrace()
        } catch (e: IllegalAccessException) {
            e.printStackTrace()
        }

        return retList
    }
}
