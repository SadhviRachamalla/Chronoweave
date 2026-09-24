package com.chronoweave.worker.messaging;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.worker.executor.JobExecutor;
import com.chronoweave.worker.service.HeartbeatPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DispatchMessageListener {

    private static final Logger log = LoggerFactory.getLogger(DispatchMessageListener.class);
    private static final String STATUS_TOPIC = "chronoweave-status";

    private final Map<String, JobExecutor> executorMap;
    private final HeartbeatPublisherService heartbeatService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DispatchMessageListener(Map<String, JobExecutor> executorMap,
                                   HeartbeatPublisherService heartbeatService,
                                   KafkaTemplate<String, Object> kafkaTemplate) {
        this.executorMap = executorMap;
        this.heartbeatService = heartbeatService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "chronoweave-dispatch", groupId = "chronoweave-worker-group")
    public void onDispatch(JobDispatchEvent event) {
        log.info("Worker {} received dispatch for job {} (type={})", heartbeatService.getWorkerId(), event.jobId(), event.jobType());
        
        heartbeatService.incrementActiveSlots();
        long startTime = System.currentTimeMillis();
        JobStatusEvent statusEvent;

        try {
            JobExecutor executor = executorMap.get(event.jobType().name());
            if (executor == null) {
                statusEvent = new JobStatusEvent(
                    event.jobId(), event.attempt(), heartbeatService.getWorkerId(),
                    JobState.FAILED, "No executor found for job type: " + event.jobType(), null,
                    System.currentTimeMillis() - startTime
                );
            } else {
                statusEvent = executor.execute(event, heartbeatService.getWorkerId());
            }

        } catch (Exception e) {
            log.error("Unhandled exception executing job {}", event.jobId(), e);
            statusEvent = new JobStatusEvent(
                event.jobId(), event.attempt(), heartbeatService.getWorkerId(),
                JobState.FAILED, "Execution error: " + e.getMessage(), null,
                System.currentTimeMillis() - startTime
            );
        } finally {
            heartbeatService.decrementActiveSlots();
        }

        kafkaTemplate.send(STATUS_TOPIC, event.jobId(), statusEvent);
        log.info("Worker {} published status event for job {}: {}", heartbeatService.getWorkerId(), event.jobId(), statusEvent.status());
    }
}
