package com.chronoweave.worker.executor;

import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.JobStatusEvent;
import com.chronoweave.shared.enums.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

@Component("HTTP_CALL")
public class HttpCallExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(HttpCallExecutor.class);
    // Safe allowlist of domains to prevent SSFR / arbitrary network calls
    private static final List<String> ALLOWED_HOSTS = List.of("httpbin.org", "jsonplaceholder.typicode.com", "localhost", "127.0.0.1");

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    @Override
    public JobStatusEvent execute(JobDispatchEvent event, String workerId) {
        long start = System.currentTimeMillis();
        String targetUrl = event.payload() != null && event.payload().contains("http") 
            ? extractUrl(event.payload()) 
            : "https://httpbin.org/get";

        try {
            URI uri = URI.create(targetUrl);
            if (!ALLOWED_HOSTS.contains(uri.getHost())) {
                return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.FAILED,
                    "Host " + uri.getHost() + " is not in safety allowlist: " + ALLOWED_HOSTS, null, System.currentTimeMillis() - start);
            }

            HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.SUCCEEDED, null,
                    "HTTP " + response.statusCode() + " body: " + response.body().substring(0, Math.min(200, response.body().length())), System.currentTimeMillis() - start);
            } else {
                return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.FAILED,
                    "HTTP request failed with status: " + response.statusCode(), null, System.currentTimeMillis() - start);
            }

        } catch (Exception e) {
            return new JobStatusEvent(event.jobId(), event.attempt(), workerId, JobState.FAILED,
                "HTTP call error: " + e.getMessage(), null, System.currentTimeMillis() - start);
        }
    }

    private String extractUrl(String payload) {
        if (payload.contains("\"url\":\"")) {
            int start = payload.indexOf("\"url\":\"") + 7;
            int end = payload.indexOf("\"", start);
            if (end > start) return payload.substring(start, end);
        }
        return "https://httpbin.org/get";
    }
}
