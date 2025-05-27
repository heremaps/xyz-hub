package com.here.xyz.jobs.steps.execution.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class S3UtilTest {

  @Test
  void s3UriTos3Path() {
    String path = S3Util.s3UriTos3Path("s3://someBucket/somePrefix/someKey");
    assertEquals("someBucket/somePrefix/someKey", path);
  }
}
