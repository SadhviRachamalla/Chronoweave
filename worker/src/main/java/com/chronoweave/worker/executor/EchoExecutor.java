package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import org.springframework.stereotype.Component;

@Component("ECHO")
public class EchoExecutor implements JobExecutor {
    @Override
    public JobStatusEvent execute(JobDispatchEvent event, String workerId) {
        long start = System.currentTimeMillis();
        String output = "ECHO: " + event.payload();
        return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.SUCCEEDED, null, output, System.currentTimeMillis() - start);
    }
}
