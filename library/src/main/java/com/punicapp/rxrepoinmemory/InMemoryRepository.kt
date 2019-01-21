package com.punicapp.rxrepoinmemory

import com.google.common.base.Optional
import com.punicapp.rxrepocore.IRepository
import com.punicapp.rxrepocore.LocalFilters
import com.punicapp.rxrepocore.LocalSorts
import io.reactivex.Single
import io.reactivex.functions.Consumer
import io.reactivex.functions.Function
import java.util.*

class InMemoryRepository<T : Any> @JvmOverloads constructor(private val copyMode: Int = NO_COPY_MODE) : IRepository<T> {
    private val storage = Collections.synchronizedSet(HashSet<T>())

    init {
        if (!(copyMode == NO_COPY_MODE || copyMode == SHALLOW_COPY_MODE))
            throw RuntimeException("Wrong copyMode " + copyMode)
    }

    override fun saveInChain(clearData: Boolean): Consumer<T> {
        return Consumer { t ->
            if (clearData) storage.clear()
            storage.add(t)
        }
    }

    override fun saveAllInChain(clearData: Boolean): Consumer<List<T>> {
        return Consumer { ts ->
            if (clearData) storage.clear()
            storage.addAll(ts)
        }
    }

    override fun modifyFirst(action: Consumer<T>): Single<T> {
        return first(null, null)
                .map { tWrapper ->
                    if (!tWrapper.isPresent) {
                        throw IllegalStateException("Modified object is null!!!")
                    }
                    tWrapper.get()
                }
                .doOnSuccess(action)
                .doOnSuccess(saveInChain())
    }

    override fun removeInChain(filters: LocalFilters?): Single<Int> {
        return Single.create(object : AbstractGetDataTask<T, Int>(storage, filters, null) {
            override fun processData(query: InMemoryQuery<T>): Int {
                return query.remove()
            }
        })
    }


    override fun fetch(filters: LocalFilters?, sorts: LocalSorts?): Single<Optional<List<T>>> {
        var single = Single.create(object : AbstractGetDataTask<T, Optional<List<T>>>(storage, filters, sorts) {
            override fun processData(query: InMemoryQuery<T>): Optional<List<T>> {
                return Optional.fromNullable(query.find())
            }
        })
        if (copyMode == SHALLOW_COPY_MODE) {
            single = single.map(Function<Optional<List<T>>, Optional<List<T>>> { wrapper ->
                if (wrapper.isPresent) {
                    return@Function Optional.of(wrapper.get().map { ObjectCloner.cloneObject(it)!! })
                }
                wrapper
            })
        }
        return single
    }

    override fun first(filters: LocalFilters?, sorts: LocalSorts?): Single<Optional<T>> {
        var single = Single.create(object : AbstractGetDataTask<T, Optional<T>>(storage, filters, sorts) {
            override fun processData(query: InMemoryQuery<T>): Optional<T> {
                return Optional.fromNullable(query.first())
            }
        })
        if (copyMode == SHALLOW_COPY_MODE) {
            single = single.map(Function<Optional<T>, Optional<T>> { wrapper ->
                if (wrapper.isPresent) Optional.fromNullable(ObjectCloner.cloneObject(wrapper.get())) else wrapper
            })
        }
        return single
    }

    override fun instantFetch(filters: LocalFilters?, sorts: LocalSorts?): Optional<List<T>> {
        val initial = fetch(filters, sorts)
        return initial.blockingGet()
    }

    override fun instantFirst(filters: LocalFilters?, sorts: LocalSorts?): Optional<T> {
        val initial = first(filters, sorts)
        return initial.blockingGet()
    }

    override fun count(filters: LocalFilters?): Single<Long> {
        return Single.create(object : AbstractGetDataTask<T, Long>(storage, filters, null) {
            override fun processData(query: InMemoryQuery<T>): Long {
                return query.count()
            }
        })
    }

    override fun instantCount(filters: LocalFilters?): Long? {
        val initial = count(filters)
        return initial.blockingGet()
    }

    companion object {
        val NO_COPY_MODE = 1
        val SHALLOW_COPY_MODE = 2
    }
}
