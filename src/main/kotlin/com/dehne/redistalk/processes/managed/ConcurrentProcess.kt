package com.dehne.redistalk.processes.managed

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import redis.clients.jedis.Jedis
import java.time.Instant

abstract class ConcurrentProcess(
    val processName: String = "KafkaProcessTopic1",
    private val jedis: Jedis,
    private val objectMapper: ObjectMapper,
) {

    val myNodeName = System.getenv("NODE_NAME")
    private val workers: List<Worker> = mutableListOf()

    fun heartBeat() { // runs every 10 seconds
        var startOrStopWorkers = 0
        val config = getConfig()

        readWriteModify(processName) { clusterState ->
            startOrStopWorkers = calculateWorkers(
                need = if (config.running) config.workers else 0,
                activeRemote = clusterState.validNeighbors(myNodeName).sumOf { it.activeWorkers },
                activeLocally = workers.size
            )

            clusterState.update(
                NodeState(
                    nodeName = myNodeName,
                    heartbeat = Instant.now(),
                    activeWorkers = workers.size
                )
            )
        }

        if (startOrStopWorkers != 0) {
            startOrStopWorkers(startOrStopWorkers)
        }
    }

    private fun readWriteModify(processName: String, fn: (existing: ClusterState) -> ClusterState?) {
        val key = namespaceKey(processName)

        for (i in 0..100) {
            jedis.watch(key)

            val existing = jedis.get(key)?.let {
                objectMapper.readValue(it)
            } ?: ClusterState()

            val updated = fn(existing) ?: return

            val ok = jedis.multi().let { tx ->
                tx.set(key, objectMapper.writeValueAsString(updated))
                tx.exec().size == 1
            }

            if (ok) return
        }
        error("Too many retries")
    }

    private fun namespaceKey(processName: String) = "${this::class.java.name}-clusterState-$processName"

    private fun startOrStopWorkers(startOrStopWorkers: Int) {
        TODO("Not yet implemented")
    }

    private fun startWorkers(toBeStarted: Int) {
        TODO("Not yet implemented")
    }

    private fun calculateWorkers(
        need: Int,
        activeRemote: Int,
        activeLocally: Int,
    ): Int {
        TODO("Not yet implemented")
    }

    private fun getConfig(): MyConfig {
        TODO("Not yet implemented")
    }

}

data class MyConfig(
    val workers: Int = 1,
    val running: Boolean = false
)

class Worker : Thread()

