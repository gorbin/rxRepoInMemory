package com.punicapp.rxrepoinmemory

import android.util.Log

import java.lang.reflect.Field

internal object ObjectCloner {
    fun <T: Any> cloneObject(obj: T): T? {
        try {
            val clazz = obj.javaClass
            val clone = clazz.newInstance() as T
            for (field in clazz.declaredFields) {
                field.isAccessible = true
                field.set(clone, field.get(obj))
            }
            return clone
        } catch (e: Exception) {
            Log.w("Exception", e.toString(), e)
            return null
        }

    }
}
