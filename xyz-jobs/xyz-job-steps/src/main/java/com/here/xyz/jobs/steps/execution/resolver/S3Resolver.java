package com.here.xyz.jobs.steps.execution.resolver;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class S3Resolver implements ScriptTokenResolver {

  private static final Logger logger = LogManager.getLogger();
  private final String localDirectory;
  Map<String, String> s3UriToLocalDir = new HashMap<>();

  public S3Resolver(String localDirectory) {
    this.localDirectory = localDirectory;
  }

  public String registerS3Uri(String s3Uri) {
    if (!s3UriToLocalDir.containsKey(s3Uri)) {
      String s3Path = S3Util.s3UriTos3Path(s3Uri);
      final String localRootPath = "/tmp/";
      String localPath = localRootPath + s3Path;

      s3UriToLocalDir.put(s3Uri, localPath);
    }
    return s3UriToLocalDir.get(s3Uri);
  }

  @Override
  public List<String> resolveScriptParams(List<String> scriptParams) {
    return scriptParams.stream().map(this::resolveScriptParam).toList();
  }

  public String resolveScriptParam(String scriptParam) {
    String resolvedParam = scriptParam;
    Matcher matcher = S3Util.S3_URI_PATTERN.matcher(scriptParam);
    while (matcher.find()) {
      String s3Uri = matcher.group();
      String localDirectory = registerS3Uri(s3Uri);
      resolvedParam = resolvedParam.replaceAll(s3Uri, localDirectory);
    }
    return resolvedParam;
  }


  public void createLocalDirectories() {
    s3UriToLocalDir.forEach((s3Uri, localDir) -> {
      try {
        Files.createDirectories(Path.of(localDir));
      } catch (IOException e) {
        throw new RuntimeException("Failed to create local directory for S3 key. s3Uri=" + s3Uri + " localDir=" + localDir, e);
      }
    });
  }

  public void uploadLocalFilesToS3() {
    s3UriToLocalDir.forEach((s3Uri, localDir) -> {
      logger.info("[EMR-local] Uploading localDir={} to s3Uri={}...", localDir, s3Uri);
      String s3TargetPath = S3Util.s3UriTos3Path(s3Uri);
      S3Util.uploadDirectoryToS3(new File(localDir), s3TargetPath);
    });
  }

  public void downloadFromS3() {
    s3UriToLocalDir.forEach((s3Uri, localDir) -> {
      logger.info("[EMR-local] Downloading from s3Uri={} to localDir={}...", s3Uri, localDir);
      String s3TargetPath = S3Util.s3UriTos3Path(s3Uri);
      S3Util.copyFileFromS3ToLocal(s3TargetPath, localDirectory);
    });
  }
}
