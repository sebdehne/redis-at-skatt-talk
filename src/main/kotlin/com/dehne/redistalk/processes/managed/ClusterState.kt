package com.dehne.redistalk.processes.managed

import java.time.Instant

data class ClusterState(
    val nodes: Map<String, NodeState> = emptyMap()
) {
    fun validNeighbors(myNodeName: String) = nodes.values
        .filter { it.isAlive() && it.nodeName != myNodeName }

    fun update(n: NodeState) = copy(
        nodes = nodes.filter { it.value.isAlive() } + (n.nodeName to n))
}

data class NodeState(
    val nodeName: String,
    val heartbeat: Instant,
    val activeWorkers: Int,
)

fun NodeState.isAlive() = heartbeat.plusSeconds(30).isAfter(Instant.now())

