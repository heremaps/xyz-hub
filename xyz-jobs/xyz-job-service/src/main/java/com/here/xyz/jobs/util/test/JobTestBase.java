package com.here.xyz.jobs.util.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.RuntimeStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.net.http.HttpClient.Redirect.NORMAL;

public class JobTestBase extends StepTestBase {
    private static final Logger logger = LogManager.getLogger();

    /** Job-Api related */
    public static String createJob(Job job) throws IOException, InterruptedException {
        logger.info("Creating job ...");
        HttpResponse<byte[]> jobResponse = post("/jobs", job);

        logger.info("Got response:");
        logger.info(new String(jobResponse.body()));

        Job createdJob = XyzSerializable.deserialize(jobResponse.body(), Job.class);
        return createdJob.getId();
    }

    public static void uploadFileToJob(String jobId, byte[] fileContent) throws IOException, InterruptedException {
        HttpResponse<byte[]> inputResponse = post("/jobs/" + jobId + "/inputs", Map.of("type", "UploadUrl"));
        String uploadUrl = (String) XyzSerializable.deserialize(inputResponse.body(), Map.class).get("url");
        uploadUrl = uploadUrl.replace("localstack","localhost");
        uploadInputFile(fileContent, new URL(uploadUrl));
    }

    public static void uploadFilesToJob(String jobId, List<byte[]> fileContents) throws IOException, InterruptedException {
        //Generate N Files with M features
        for(byte[] fileContent :fileContents) {
            uploadFileToJob(jobId, fileContent);
        }
    }

    public static void startJob(String jobId) throws IOException, InterruptedException {
        logger.info("Starting job ...");
        patch("/jobs/" + jobId + "/status", Map.of("desiredAction", "START"));
    }

    public static void deleteJob(String jobId) throws IOException, InterruptedException {
        logger.info("Deleting job ...");
        delete("/jobs/" + jobId );
    }

    public static Map getJob(String jobId, boolean useAdminEndpoint) throws IOException, InterruptedException {
        logger.info("Get job ...");
        HttpResponse<byte[]> jobResponse = get((useAdminEndpoint ? "/admin" : "/") +"/jobs/" + jobId);
        return XyzSerializable.deserialize(jobResponse.body(), new TypeReference<Map>() {});
    }

    public static RuntimeStatus getJobStatus(String jobId) throws IOException, InterruptedException {
        logger.info("Get job status...");
        HttpResponse<byte[]> statusResponse = get("/jobs/" + jobId + "/status");
        return XyzSerializable.deserialize(statusResponse.body(), RuntimeStatus.class);
    }

    public static List<Map> getJobOutputs(String jobId) throws IOException, InterruptedException {
        logger.info("Get job Outputs ...");
        HttpResponse<byte[]> outputResponse = get("/jobs/" + jobId + "/outputs");
        return XyzSerializable.deserialize(outputResponse.body(), new TypeReference<List<Map>>() {});
    }

    public static void pollJobStatus(String jobId) throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        //Poll job status every 5 seconds
        executor.scheduleAtFixedRate(() -> {
            try {
                RuntimeStatus status = getJobStatus(jobId);
                logger.info("Job state for {}: {} ({}/{} steps succeeded)", jobId, status.getState(), status.getSucceededSteps(),
                        status.getOverallStepCount());
                if (status.getState().isFinal()) {
                    if(!status.getState().equals(RuntimeInfo.State.SUCCEEDED))
                        logger.info("Job state for {} is not SUCCEEDED: {}", jobId, XyzSerializable.serialize(status));
                    executor.shutdownNow();
                }
            }
            catch (Exception e) {
                logger.error(e);
                throw new RuntimeException(e);
            }
        }, 0, 5, TimeUnit.SECONDS);

        int timeoutSeconds = 120;
        if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            logger.info("Stopped polling status for job {} after timeout {} seconds", jobId, timeoutSeconds);
        }
    }

    private static HttpResponse<byte[]> get(String path) throws IOException, InterruptedException {
        return request("GET", path, null);
    }

    private static HttpResponse<byte[]> post(String path, Object requestPayload) throws IOException, InterruptedException {
        return request("POST", path, requestPayload);
    }

    private static HttpResponse<byte[]> patch(String path, Object requestPayload) throws IOException, InterruptedException {
        return request("PATCH", path, requestPayload);
    }

    private static HttpResponse<byte[]> delete(String path) throws IOException, InterruptedException {
        return request("DELETE", path, null);
    }

    private static HttpResponse<byte[]> request(String method, String path, Object requestPayload) throws IOException, InterruptedException {
        HttpRequest.BodyPublisher bodyPublisher = requestPayload == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(XyzSerializable.serialize(requestPayload).getBytes());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(config.JOB_API_ENDPOINT + path))
                .header(CONTENT_TYPE, JSON_UTF_8.toString())
                .method(method, bodyPublisher)
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        HttpClient client = HttpClient.newBuilder().followRedirects(NORMAL).build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400)
            throw new RuntimeException("Received error response with status code: " + response.statusCode() + " response:\n"
                    + new String(response.body()));
        return response;
    }

    private static void uploadInputFile(byte[] data, URL uploadUrl) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uploadUrl.openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestMethod("PUT");
        OutputStream out = connection.getOutputStream();

        out.write(data);
        out.close();

        if (connection.getResponseCode() < 200 || connection.getResponseCode() > 299)
            throw new RuntimeException("Error uploading file, got status code " + connection.getResponseCode());
    }

    protected void createSelfRunningJob(Job job) throws Exception {
        /** Create Job - expect autostart */
        createJob(job);
        /** Wait till Job reached final state */
        pollJobStatus(job.getId());
    }

    protected void createAndStartJob(Job job, byte[] fileContent) throws Exception {
        /** Create Job */
        createJob(job);
        /** Upload content if provided */
        if(fileContent != null)
            uploadFileToJob(job.getId(), fileContent);
        /** Start Job execution */
        startJob(job.getId());
        /** Wait till Job reached final state */
        pollJobStatus(job.getId());

    }
}
