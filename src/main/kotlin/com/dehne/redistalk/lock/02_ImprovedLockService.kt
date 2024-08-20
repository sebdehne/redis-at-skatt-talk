package com.dehne.redistalk.lock

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import redis.clients.jedis.Jedis
import java.time.Instant
import java.util.*

@Suppress("DuplicatedCode")
class ImprovedLockService(
    private val objectMapper: ObjectMapper,
    private val jedis: Jedis,
) {

    fun tryRunLocked(
        lock: String,
        expiresSeconds: Long = 30,
        fn: () -> Unit
    ): Boolean {
        val lockId = lock(lock, expiresSeconds) ?: return false
        return try {
            fn()
            true
        } finally {
            unlock(lock, lockId)
        }
    }

    private fun lock(lock: String, expiresSeconds: Long = 30): String? {
        val now = Instant.now()
        var lockId: String? = null

        readModifyWrite(lock) { existingLock ->
            if (!existingLock.isAvailable(now.toEpochMilli())) {
                return@readModifyWrite null
            }

            Lock(
                id = UUID.randomUUID().toString(),
                ownerHost = System.getenv("POD_NAME") ?: "unknown",
                ownerThread = Thread.currentThread().name,
                validUntil = now.toEpochMilli() + (expiresSeconds * 1000),
                updatedAt = now
            ).apply { lockId = id }
        }

        return lockId
    }

    private fun unlock(lock: String, id: String) {
        readModifyWrite(lock) { existingLock ->
            if (existingLock?.id != id) {
                null
            } else {
                existingLock.copy(
                    validUntil = 0,
                    updatedAt = Instant.now()
                )
            }
        }
    }

    private fun readModifyWrite(lock: String, fn: (existing: Lock?) -> Lock?): Boolean {
        val key = namespaceKey(lock)

        for (i in 0..100) {
            jedis.watch(key)
            val existing = jedis.get(key)?.let {
                objectMapper.readValue<Lock>(it)
            }

            val updated = fn(existing) ?: return false

            val ok = jedis.multi().let { tx ->
                tx.set(
                    key,
                    objectMapper.writeValueAsString(updated)
                )
                tx.exec().size == 1
            }

            if (ok) return true
        }
        error("Too many retries")
    }

    private fun namespaceKey(lock: String) = "${this::class.java.name}-$lock"
}


data class Lock(
    val id: String,
    val ownerHost: String,
    val ownerThread: String,
    val validUntil: Long,
    val updatedAt: Instant,
)

fun Lock?.isAvailable(time: Long) = if (this == null) true else validUntil < time

