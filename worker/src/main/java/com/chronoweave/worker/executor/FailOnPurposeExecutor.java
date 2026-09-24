package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import org.springframework.stereotype.Component;

@Component("FAIL_ON_PURPOSE")
public class FailOnPurposeExecutor implements JobExecutor {
    @Override
    public JobStatusEvent execute(JobDispatchEvent event, String workerId) {
        long start = System.currentTimeMillis();
        return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.FAILED, "Simulated intentional failure for testing retries and DLQ", null, System.currentTimeMillis() - start);
    }
}
