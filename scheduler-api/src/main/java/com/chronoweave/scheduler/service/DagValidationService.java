package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.shared.exception.DagCycleException;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DagValidationService {

    public void validateNoCycles(JobEntity newJob, Set<JobEntity> parentJobs) {
        Map<String, Set<String>> graph = new HashMap<>();
        
        // Add dependency edge: child -> parent (newJob relies on parentJobs)
        Set<String> parentIds = new HashSet<>();
        for (JobEntity parent : parentJobs) {
            parentIds.add(parent.getId());
            // Traverse parent dependencies recursively to build graph
            buildGraph(parent, graph);
        }
        graph.put(newJob.getId(), parentIds);

        // Run cycle detection (DFS with visited states: 0=unvisited, 1=visiting, 2=visited)
        Map<String, Integer> stateMap = new HashMap<>();
        for (String node : graph.keySet()) {
            if (hasCycleDFS(node, graph, stateMap)) {
                throw new DagCycleException("Cycle detected in DAG workflow dependency graph for job " + newJob.getId());
            }
        }
    }

    private void buildGraph(JobEntity node, Map<String, Set<String>> graph) {
        if (graph.containsKey(node.getId())) return;
        Set<String> parents = new HashSet<>();
        if (node.getDependencies() != null) {
            for (JobEntity p : node.getDependencies()) {
                parents.add(p.getId());
                buildGraph(p, graph);
            }
        }
        graph.put(node.getId(), parents);
    }

    private boolean hasCycleDFS(String current, Map<String, Set<String>> graph, Map<String, Integer> stateMap) {
        stateMap.put(current, 1); // Visiting
        Set<String> neighbors = graph.getOrDefault(current, Collections.emptySet());
        for (String neighbor : neighbors) {
            int state = stateMap.getOrDefault(neighbor, 0);
            if (state == 1) {
                return true; // Found back-edge -> cycle
            }
            if (state == 0 && hasCycleDFS(neighbor, graph, stateMap)) {
                return true;
            }
        }
        stateMap.put(current, 2); // Visited
        return false;
    }
}
