package com.chronoweave.scheduler.service;

import com.chronoweave.shared.dto.WorkerHeartbeat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class WorkerRegistryService {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistryService.class);
    private static final String ACTIVE_WORKERS_SET = "chronoweave:active_workers";
    private static final String WORKER_HASH_PREFIX = "chronoweave:workers:";
    private static final long STALE_THRESHOLD_MS = 15000; // 15 seconds

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;

    public WorkerRegistryService(RedissonClient redissonClient, ObjectMapper objectMapper) {
        this.redissonClient = redissonClient;
        this.objectMapper = objectMapper;
    }

    public void registerHeartbeat(WorkerHeartbeat heartbeat) {
        try {
            String json = objectMapper.writeValueAsString(heartbeat);
            RMap<String, String> map = redissonClient.getMap(WORKER_HASH_PREFIX + heartbeat.workerId());
            map.put("meta", json);
            map.put("lastHeartbeat", String.valueOf(Instant.now().toEpochMilli()));

            RSet<String> set = redissonClient.getSet(ACTIVE_WORKERS_SET);
            set.add(heartbeat.workerId());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize worker heartbeat", e);
        }
    }

    public List<WorkerHeartbeat> getActiveWorkers() {
        RSet<String> set = redissonClient.getSet(ACTIVE_WORKERS_SET);
        List<WorkerHeartbeat> active = new ArrayList<>();
        long now = Instant.now().toEpochMilli();

        for (String workerId : set) {
            RMap<String, String> map = redissonClient.getMap(WORKER_HASH_PREFIX + workerId);
            String lastHbStr = map.get("lastHeartbeat");
            String json = map.get("meta");

            if (lastHbStr != null && json != null) {
                long lastHb = Long.parseLong(lastHbStr);
                if ((now - lastHb) <= STALE_THRESHOLD_MS) {
                    try {
                        WorkerHeartbeat hb = objectMapper.readValue(json, WorkerHeartbeat.class);
                        active.add(hb);
                    } catch (JsonProcessingException e) {
                        log.error("Failed to deserialize heartbeat for worker {}", workerId, e);
                    }
                }
            }
        }
        return active;
    }

    public List<String> getStaleWorkerIds() {
        RSet<String> set = redissonClient.getSet(ACTIVE_WORKERS_SET);
        List<String> stale = new ArrayList<>();
        long now = Instant.now().toEpochMilli();

        for (String workerId : set) {
            RMap<String, String> map = redissonClient.getMap(WORKER_HASH_PREFIX + workerId);
            String lastHbStr = map.get("lastHeartbeat");
            if (lastHbStr == null || (now - Long.parseLong(lastHbStr)) > STALE_THRESHOLD_MS) {
                stale.add(workerId);
            }
        }
        return stale;
    }

    public void unregisterWorker(String workerId) {
        redissonClient.getSet(ACTIVE_WORKERS_SET).remove(workerId);
        redissonClient.getMap(WORKER_HASH_PREFIX + workerId).delete();
    }
}
