package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import org.springframework.stereotype.Component;

@Component("SLEEP")
public class SleepExecutor implements JobExecutor {
    @Override
    public JobStatusEvent execute(JobDispatchEvent event, String workerId) {
        long start = System.currentTimeMillis();
        try {
            // Sleep for 1 second as default simulated work
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.FAILED, "Interrupted", null, System.currentTimeMillis() - start);
        }
        return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.SUCCEEDED, null, "Slept 1000ms", System.currentTimeMillis() - start);
    }
}
