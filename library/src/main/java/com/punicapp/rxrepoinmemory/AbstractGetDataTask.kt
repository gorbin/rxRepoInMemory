package com.punicapp.rxrepoinmemory

import com.punicapp.rxrepocore.LocalFilters
import com.punicapp.rxrepocore.LocalSorts

import io.reactivex.SingleEmitter
import io.reactivex.SingleOnSubscribe

internal abstract class AbstractGetDataTask<T : Any, U>(private val storage: MutableCollection<T>, private val filters: LocalFilters?, private val sorts: LocalSorts?) : SingleOnSubscribe<U> {

    override fun subscribe(singleEmitter: SingleEmitter<U>) {
        val query = InMemoryQuery(storage)
        filters?.filters?.apply(query::filter)
        sorts?.sorts?.apply(query::sort)
        val data = processData(query)
        singleEmitter.onSuccess(data)
    }

    protected abstract fun processData(query: InMemoryQuery<T>): U
}
