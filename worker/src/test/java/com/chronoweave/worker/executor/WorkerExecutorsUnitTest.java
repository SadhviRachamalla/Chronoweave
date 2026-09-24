package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.enums.JobType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WorkerExecutorsUnitTest {

    @Test
    void testEchoExecutorSuccess() {
        EchoExecutor executor = new EchoExecutor();
        JobDispatchEvent event = new JobDispatchEvent("job-1", JobType.ECHO, "Hello Worker", "DEFAULT", 1, 1000L);
        JobStatusEvent result = executor.execute(event, "worker-test");

        assertEquals(JobState.SUCCEEDED, result.status());
        assertTrue(result.outputData().contains("Hello Worker"));
    }

    @Test
    void testFailOnPurposeExecutor() {
        FailOnPurposeExecutor executor = new FailOnPurposeExecutor();
        JobDispatchEvent event = new JobDispatchEvent("job-2", JobType.FAIL_ON_PURPOSE, "{}", "DEFAULT", 1, 1000L);
        JobStatusEvent result = executor.execute(event, "worker-test");

        assertEquals(JobState.FAILED, result.status());
        assertNotNull(result.errorMessage());
    }
}
