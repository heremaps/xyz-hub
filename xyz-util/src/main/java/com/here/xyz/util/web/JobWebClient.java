package com.here.xyz.util.web;

import io.vertx.core.json.JsonObject;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.time.temporal.ChronoUnit.SECONDS;

public class JobWebClient extends XyzWebClient {
    private static Map<InstanceKey, JobWebClient> instances = new ConcurrentHashMap<>();
    public static String userAgent = DEFAULT_USER_AGENT;

    public JobWebClient(String baseUrl) {
        super(baseUrl, userAgent);
    }

    protected JobWebClient(String baseUrl, Map<String, String> extraHeaders) {
        super(baseUrl, userAgent, extraHeaders);
    }

    @Override
    public boolean isServiceReachable() {
        try {
            request(HttpRequest.newBuilder()
                    .uri(uri("/health"))
                    .timeout(Duration.of(3, SECONDS)));
        }
        catch (XyzWebClient.WebClientException e) {
            return false;
        }
        return true;
    }

    public static JobWebClient getInstance(String baseUrl) {
        return getInstance(baseUrl, null);
    }

    public static JobWebClient getInstance(String baseUrl, Map<String, String> extraHeaders) {
        InstanceKey key = new InstanceKey(baseUrl, extraHeaders);
        if (!instances.containsKey(key))
            instances.put(key, new JobWebClient(baseUrl, extraHeaders));
        return instances.get(key);
    }

    public JsonObject createJob(JsonObject job) throws XyzWebClient.WebClientException {
        return new JsonObject(new String(request(HttpRequest.newBuilder()
            .uri(uri("/jobs"))
            .header(CONTENT_TYPE, JSON_UTF_8.toString())
            .method("POST", HttpRequest.BodyPublishers.ofByteArray(job.toString().getBytes())))
            .body()));
    }
}
