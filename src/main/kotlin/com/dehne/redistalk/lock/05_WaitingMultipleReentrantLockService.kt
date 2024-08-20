package com.dehne.redistalk.lock

import com.dehne.redistalk.groupchat.GroupChatService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import redis.clients.jedis.Jedis
import java.time.Duration
import java.time.Instant
import java.util.*

@Suppress("DuplicatedCode")
class WaitingMultipleReentrantLockService(
    private val objectMapper: ObjectMapper,
    private val jedis: Jedis,
    private val groupChatService: GroupChatService,
) {

    fun tryRunLocked(
        locks: Set<String>,
        timeout: Duration,
        lockTTL: Long = 30,
        fn: () -> Unit
    ): Boolean {
        if (tryRunLocked(locks, lockTTL, fn)) return true

        val deadLine = System.nanoTime() + timeout.toNanos()
        val sub = groupChatService.subscribe(channelIds = locks.map { namespaceKey(it) }.toSet())
        try {

            while (System.nanoTime() < deadLine) {
                if (tryRunLocked(locks, lockTTL, fn)) return true

                sub.poll(Duration.ofSeconds(10))
            }
            return false

        } finally {
            sub.unsubscribe()
        }
    }

    fun tryRunLocked(
        locks: Set<String>,
        expiresSeconds: Long = 30,
        fn: () -> Unit
    ): Boolean {
        var lockId: String? = null

        if (reentrant.get() == null) {
            reentrant.set(LinkedList())
        }
        val newLocks = locks - (reentrant.get()!!.flatten().toSet())

        if (newLocks.isNotEmpty()) { // first time entry
            lockId = tryRunLocked(newLocks, expiresSeconds) ?: return false
        }
        reentrant.get()!!.add(newLocks)

        return try {
            fn()
            true
        } finally {
            if (lockId != null) {
                unlock(newLocks, lockId)
            }
            reentrant.get()!!.removeLast()
            if (reentrant.get()!!.isEmpty()) {
                reentrant.set(null) // last exit, cleanup
            }
        }
    }

    private fun tryRunLocked(locks: Set<String>, expiresSeconds: Long = 30): String? {
        val now = Instant.now()
        var lockId: String? = null

        readModifyWrite(locks) { existing ->
            if (!existing.values.all { it.isAvailable(now.toEpochMilli()) }) {
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

    private fun unlock(locks: Set<String>, id: String) {
        var unlocked = false
        readModifyWrite(locks) { existing ->

            if (existing.values.all { it?.id == id }) {
                unlocked = true
                existing.values.first()!!.copy(
                    validUntil = 0,
                    updatedAt = Instant.now()
                )
            } else {
                null
            }
        }
        if (unlocked) {
            groupChatService.notify(channelIds = locks.map { namespaceKey(it) }.toSet(), msg = "unlocked")
        }
    }

    private fun readModifyWrite(locks: Set<String>, fn: (existing: Map<String, Lock?>) -> Lock?): Boolean {
        val keys = locks.map { namespaceKey(it) }

        for (i in 0..100) {
            jedis.watch(*keys.toTypedArray())

            val existing = keys.associateWith { l ->
                jedis.get(l)?.let { objectMapper.readValue<Lock>(it) }
            }

            val updated = fn(existing) ?: return false

            val ok = jedis.multi().let { tx ->
                keys.forEach { l -> tx.set(l, objectMapper.writeValueAsString(updated)) }

                tx.exec().size == keys.size
            }

            if (ok) return true
        }
        error("Too many retries")
    }

    private fun namespaceKey(lock: String) = "${this::class.java.name}-$lock"

    private val reentrant: ThreadLocal<LinkedList<Set<String>>> = ThreadLocal()

}