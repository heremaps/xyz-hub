package com.here.xyz.jobs.steps.execution.resolver;


import com.google.common.base.Preconditions;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.aws.S3ObjectSummary;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Util {

  public static final Pattern S3_URI_PATTERN = Pattern.compile("s3://([a-zA-Z0-9.-]+)/([a-zA-Z0-9-_./]+)");
  private static final Map<String, String> CONTENT_TYPE_MAP = new HashMap<>() {{
    put(".geojson", "application/json");
    put(".json", "application/json");
    put(".csv", "text/csv");
    put(".txt", "text/plain");
  }};
  private static final Logger logger = LogManager.getLogger();

  public static String copyDirectoryFromS3ToLocal(String s3Path, String localDirectory) {
    //TODO: Use inputs loading of framework instead
    List<S3ObjectSummary> s3ObjectSummaries = S3Client.getInstance().scanFolder(s3Path);

    for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
      if (!s3ObjectSummary.key().contains("modelBased")) {
        copyFileFromS3ToLocal(s3ObjectSummary.key(), localDirectory);
      }
    }
    return getLocalTmpPath(localDirectory, s3Path);
  }

  public static String copyFileFromS3ToLocal(String s3Path, String localDirectory) {
    //Lambda allows writing to /tmp directory - Jar file could be bigger than 512MB
    try {
      logger.info("[EMR-local] Copy file: '{}' to local.", s3Path);
      InputStream jarStream = S3Client.getInstance().streamObjectContent(s3Path);

      //Create local target directory
      createLocalDirectoryForS3Path(localDirectory, Paths.get(s3Path).getParent().toString(), false);
      Files.copy(jarStream, Paths.get(getLocalTmpPath(localDirectory, s3Path)));
      jarStream.close();
    } catch (FileAlreadyExistsException e) {
      logger.info("[EMR-local] File: '{}' already exists locally - skip download.", s3Path);
    } catch (S3Exception e) {
      throw new RuntimeException("[EMR-local] Can't download File: '" + s3Path + "' for local copy!", e);
    } catch (IOException e) {
      throw new RuntimeException("[EMR-local] Can't copy File: '" + s3Path + "'!", e);
    }
    return getLocalTmpPath(localDirectory, s3Path);
  }

  public static String createLocalDirectoryForS3Path(String localDirectory, String s3Path, boolean deleteBefore) throws IOException {
    String tmpPath = getLocalTmpPath(localDirectory, s3Path);
    Path path = Paths.get(tmpPath);

    if (deleteBefore) {
      deleteDirectory(path.getParent().toFile());
    }

    logger.info("[EMR-local] Create tmp dir: {}", path);
    Files.createDirectories(path);

    return tmpPath;
  }

  private static String getLocalTmpPath(String localTempRootPath, String s3Path) {
    // Example: localTempRootPath = "/tmp/";
    Preconditions.checkState(localTempRootPath.startsWith("/"));
    Preconditions.checkState(localTempRootPath.endsWith("/"));
    return localTempRootPath + s3Path;
  }

  public static void deleteDirectory(File directory) {
    if (directory.isDirectory()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    directory.delete();
  }

  public static String s3UriTos3Path(String s3Uri) {
    String bucket = S3Client.getBucketFromS3Uri(s3Uri);
    String key = S3Client.getKeyFromS3Uri(s3Uri);
    return bucket + "/" + key;
  }

  public static void uploadDirectoryToS3(File localDirectoryToUpload, String s3TargetPath) {
    if (localDirectoryToUpload.exists() && localDirectoryToUpload.isDirectory()) {
      File[] files = localDirectoryToUpload.listFiles();

      if (files == null) {
        logger.info("[EMR-local] EMR job has not produced any files!");
        return;
      }

      for (File file : files) {
        if (file.getPath().endsWith("crc") || file.getName().equalsIgnoreCase("_SUCCESS")) {
          continue;
        }

        if (file.isDirectory()) {
          logger.info("[EMR-local] Directory detected {} ", file);
          uploadDirectoryToS3(file, s3TargetPath + file.getName());
          continue;
        }

        logger.info("[EMR-local] Store local file {} to {} ", file, s3TargetPath);
        s3TargetPath = s3TargetPath.replaceAll("/$", "");
        Path filePath = file.toPath();
        try {
          new DownloadUrl()
              .withContentType(getContentTypeFromFileExtension(filePath))
              .withContent(Files.readAllBytes(filePath))
              .store(s3TargetPath + "/" + file.getName());
        } catch (IOException e) {
          throw new RuntimeException(
              String.format("Failed to upload directory to S3. fromLocalDir=%s s3TargetPath=%s", filePath, s3TargetPath), e);
        }
      }
    }
  }

  private static String getContentTypeFromFileExtension(Path filePath) {
    String fileName = filePath.getFileName().toString().toLowerCase();
    return CONTENT_TYPE_MAP.entrySet().stream()
        .filter(entry -> fileName.endsWith(entry.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse("text/plain"); // Default to text/plain for unknown types
  }
}
