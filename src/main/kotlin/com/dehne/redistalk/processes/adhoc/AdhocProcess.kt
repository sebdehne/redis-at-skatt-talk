package com.dehne.redistalk.processes.adhoc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import java.time.Instant
import java.util.concurrent.ExecutorService

abstract class AdhocProcess<I, O>(
    private val objectMapper: ObjectMapper,
    private val jedisSource: () -> Jedis,
    private val executorService: ExecutorService,
    private val ttlInSeconds: Long = 300,
) {

    abstract fun inputArgumentsToStableId(inputArguments: I): String
    abstract fun runAdhocTask(inputArguments: I, updateProgress: (progress: String?) -> Unit): O

    // Polled by the client
    fun startOrGetStatus(inputArguments: I, restartIfNotRunning: Boolean = false): AdhocProcessState<O>? {
        var start = false
        val id = inputArgumentsToStableId(inputArguments)

        var status: AdhocProcessState<O>? = AdhocProcessState(id)

        readModifyWrite(id) { existing ->
            if ((existing == null || existing.stoppedAt != null) && restartIfNotRunning) {
                start = true
                status
            } else {
                status = existing
                null
            }
        }

        if (start) {
            executorService.submit { start(id, inputArguments) }
        }

        return status
    }

    private fun start(id: String, inputArguments: I) {
        val result = try {
            runAdhocTask(inputArguments) { progress ->
                readModifyWrite(id) {
                    it?.copy(progress = progress)
                }
            }
        } catch (e: Exception) {
            readModifyWrite(id) { existing ->
                existing?.copy(
                    stoppedAt = Instant.now(),
                    error = e.localizedMessage,
                )
            }
            return
        }

        readModifyWrite(id) { it?.copy(stoppedAt = Instant.now(), result = result) }
    }
    private fun readModifyWrite(id: String, fn: (existing: AdhocProcessState<O>?) -> AdhocProcessState<O>?): Boolean {
        val key = namespaceKey(id)

        jedisSource().use { jedis ->
            for (i in 0..100) {
                jedis.watch(key)
                val existing = jedis.get(key)?.let { objectMapper.readValue<AdhocProcessState<O>>(it) }
                val updated = fn(existing) ?: return false
                val ok = jedis.multi().let { tx ->
                    tx.set(key, objectMapper.writeValueAsString(updated), SetParams().ex(ttlInSeconds))
                    tx.exec().size == 1
                }

                if (ok) return true
            }
        }

        error("Too many retries")
    }
    private fun namespaceKey(id: String) = "${this::class.java.name}-$id"
}

data class AdhocProcessState<T>(
    val id: String,
    val startedAt: Instant = Instant.now(),
    val stoppedAt: Instant? = null,
    val progress: String? = null,
    val error: String? = null,
    val result: T? = null
)
