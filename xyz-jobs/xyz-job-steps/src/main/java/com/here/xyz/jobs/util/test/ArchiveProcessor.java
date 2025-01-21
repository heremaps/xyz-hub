package com.here.xyz.jobs.util.test;

import java.io.*;

import com.google.common.net.MediaType;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArchiveProcessor {

    private final byte[] archiveBytes;

    private ArchiveProcessor(byte[] archiveBytes) {
        this.archiveBytes = archiveBytes;
    }

    public byte[] getArchiveBytes() {
        return archiveBytes;
    }

    public static ArchiveProcessor downloadFromUrl(URL url, boolean isCompressed, MediaType mediaType)
            throws IOException, URISyntaxException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(url.toURI())
                .header("Content-Type", mediaType.toString())
                .method("GET", HttpRequest.BodyPublishers.noBody())
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() >= 400) {
            throw new RuntimeException("Error while downloading the archive: " + response.statusCode());
        }

        InputStream dataStream = response.body();
        if (isCompressed) {
            dataStream = new GZIPInputStream(dataStream);
        }

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return new ArchiveProcessor(outputStream.toByteArray());
        }
    }

    public Map<String, List<String>> extractTextContent() throws IOException {
        Map<String, List<String>> filesContent = new HashMap<>();
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(archiveBytes);
             ZipInputStream zipInputStream = new ZipInputStream(byteArrayInputStream)) {

            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    List<String> lines = new ArrayList<>();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipInputStream));

                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                    }

                    filesContent.put(entry.getName(), lines);
                }
                zipInputStream.closeEntry();
            }
        }
        return filesContent;
    }
}
