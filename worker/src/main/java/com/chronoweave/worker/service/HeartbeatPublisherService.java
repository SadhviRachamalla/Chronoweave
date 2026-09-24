package com.chronoweave.worker.service;

import com.chronoweave.shared.dto.WorkerHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class HeartbeatPublisherService {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatPublisherService.class);

    @Value("${worker.id:worker-node-1}")
    private String workerId;

    @Value("${worker.capabilities:DEFAULT}")
    private String capabilitiesStr;

    @Value("${worker.max-slots:5}")
    private int maxSlots;

    @Value("${scheduler.api.url:http://localhost:8080}")
    private String schedulerUrl;

    private final AtomicInteger activeSlots = new AtomicInteger(0);
    private final RestTemplate restTemplate = new RestTemplate();

    @Scheduled(fixedRate = 5000)
    public void sendHeartbeat() {
        try {
            List<String> capabilities = Arrays.asList(capabilitiesStr.split(","));
            WorkerHeartbeat heartbeat = new WorkerHeartbeat(
                workerId, capabilities, maxSlots, activeSlots.get(), System.currentTimeMillis()
            );

            restTemplate.postForEntity(schedulerUrl + "/api/v1/workers/heartbeat", heartbeat, Void.class);
            log.trace("Worker {} sent heartbeat successfully", workerId);
        } catch (Exception e) {
            log.warn("Worker {} failed to send heartbeat to scheduler at {}: {}", workerId, schedulerUrl, e.getMessage());
        }
    }

    public void incrementActiveSlots() {
        activeSlots.incrementAndGet();
    }

    public void decrementActiveSlots() {
        activeSlots.decrementAndGet();
    }

    public String getWorkerId() { return workerId; }
}
