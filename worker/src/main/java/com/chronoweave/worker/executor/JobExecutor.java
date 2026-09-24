package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;

public interface JobExecutor {
    JobStatusEvent execute(JobDispatchEvent event, String workerId);
}
