package com.chronoweave.scheduler.messaging;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.scheduler.domain.JobExecutionEntity;
import com.chronoweave.scheduler.repository.JobExecutionRepository;
import com.chronoweave.scheduler.repository.JobRepository;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.util.BackoffUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class StatusEventListener {

    private static final Logger log = LoggerFactory.getLogger(StatusEventListener.class);
    private static final String DLQ_TOPIC = "chronoweave-dlq";

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public StatusEventListener(JobRepository jobRepository,
                               JobExecutionRepository executionRepository,
                               KafkaTemplate<String, Object> kafkaTemplate) {
        this.jobRepository = jobRepository;
        this.executionRepository = executionRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "chronoweave-status", groupId = "chronoweave-scheduler-group")
    @Transactional
    public void handleStatusEvent(JobStatusEvent event) {
        log.info("Received status event for job {}: status={}, attempt={}, error={}",
            event.jobId(), event.status(), event.attempt(), event.errorMessage());

        JobEntity job = jobRepository.findById(event.jobId()).orElse(null);
        if (job == null) {
            log.error("Job {} not found when processing status event", event.jobId());
            return;
        }

        // Update audit log execution entry
        List<JobExecutionEntity> executions = executionRepository.findByJobIdOrderByAttemptAsc(job.getId());
        JobExecutionEntity currentExec = executions.stream()
            .filter(e -> e.getAttempt() == event.attempt())
            .findFirst()
            .orElseGet(() -> new JobExecutionEntity(job.getId(), event.attempt(), event.workerId(), Instant.now(), event.status().name()));

        currentExec.setCompletedAt(Instant.now());
        currentExec.setStatus(event.status().name());
        currentExec.setErrorMessage(event.errorMessage());
        currentExec.setOutputData(event.outputData());
        executionRepository.save(currentExec);

        if (event.status() == JobState.SUCCEEDED) {
            job.transitionTo(JobState.SUCCEEDED);
            jobRepository.save(job);
            log.info("Job {} successfully completed", job.getId());

        } else if (event.status() == JobState.FAILED) {
            if (job.getCurrentAttempt() < job.getMaxAttempts()) {
                long backoff = BackoffUtil.calculateBackoffWithJitter(job.getCurrentAttempt(), job.getRetryBackoffMs(), 2.0);
                job.transitionTo(JobState.RETRYING);
                job.setScheduledAt(Instant.now().plusMillis(backoff));
                job.setAssignedWorkerId(null);
                job.transitionTo(JobState.QUEUED);
                jobRepository.save(job);
                log.info("Job {} failed, scheduled retry attempt {} in {} ms", job.getId(), job.getCurrentAttempt() + 1, backoff);
            } else {
                job.transitionTo(JobState.DEAD_LETTERED);
                jobRepository.save(job);
                kafkaTemplate.send(DLQ_TOPIC, job.getId(), event);
                log.error("Job {} reached max attempts ({}), moved to DEAD_LETTERED / DLQ", job.getId(), job.getMaxAttempts());
            }
        }
    }
}
