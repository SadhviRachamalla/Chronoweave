package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import org.springframework.stereotype.Component;

@Component("SIMULATED_COMPUTE")
public class SimulatedComputeExecutor implements JobExecutor {
    @Override
    public JobStatusEvent execute(JobDispatchEvent event, String workerId) {
        long start = System.currentTimeMillis();
        long sum = 0;
        for (int i = 0; i < 1_000_000; i++) {
            sum += i;
        }
        return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.SUCCEEDED, null, "Computed sum: " + sum, System.currentTimeMillis() - start);
    }
}
