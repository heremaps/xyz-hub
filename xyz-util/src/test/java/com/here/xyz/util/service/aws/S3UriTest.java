package com.here.xyz.util.service.aws;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

public class S3UriTest {

    @Test
    public void testConstructorFromURI() {
        URI uri = URI.create("s3://mybucket/mykey");
        S3Uri s3Uri = new S3Uri(uri);

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("mykey", s3Uri.key());
        assertEquals("s3://mybucket/mykey", s3Uri.uri());
    }

    @Test
    public void testConstructorFromBucketAndKey() {
        S3Uri s3Uri = new S3Uri("mybucket", "mykey");

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("mykey", s3Uri.key());
        assertEquals("s3://mybucket/mykey", s3Uri.uri());
    }

    @Test
    public void testUriMethod() {
        URI uri = URI.create("s3://mybucket/mykey");
        S3Uri s3Uri = new S3Uri(uri);
        assertEquals("s3://mybucket/mykey", s3Uri.uri());

        s3Uri = new S3Uri("anotherbucket", "path/to/object");
        assertEquals("s3://anotherbucket/path/to/object", s3Uri.uri());
    }

    @Test
    public void testHttpsAwsUrl() {
        URI uri = URI.create("https://mybucket.s3.eu-west-1.amazonaws.com/mykey");
        S3Uri s3Uri = new S3Uri(uri);

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("mykey", s3Uri.key());
        assertEquals("https://mybucket.s3.eu-west-1.amazonaws.com/mykey", s3Uri.uri());
    }

    @Test
    public void testHttpsAwsUrlWithPath() {
        S3Uri s3Uri = new S3Uri("https://mybucket.s3.us-east-1.amazonaws.com/path/to/object.json");

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("path/to/object.json", s3Uri.key());
        assertEquals("https://mybucket.s3.us-east-1.amazonaws.com/path/to/object.json", s3Uri.uri());
    }

    @Test
    public void testHttpsAwsUrlWithComplexPath() {
        S3Uri s3Uri = new S3Uri("https://complex-bucket-name.s3.ap-northeast-1.amazonaws.com/path/to/my-file_123.json");

        assertEquals("complex-bucket-name", s3Uri.bucket());
        assertEquals("path/to/my-file_123.json", s3Uri.key());
        assertEquals("https://complex-bucket-name.s3.ap-northeast-1.amazonaws.com/path/to/my-file_123.json", s3Uri.uri());
    }

    @Test
    public void testS3UriWithRootKey() {
        S3Uri s3Uri = new S3Uri("s3://mybucket/");

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("", s3Uri.key());
        assertEquals("s3://mybucket/", s3Uri.uri());
    }

    @Test
    public void testHttpsAwsUrlWithRootKey() {
        S3Uri s3Uri = new S3Uri("https://mybucket.s3.us-west-1.amazonaws.com/");

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("", s3Uri.key());
        assertEquals("https://mybucket.s3.us-west-1.amazonaws.com/", s3Uri.uri());
    }

    @Test
    public void testBucketAndKeyToHttpsUri() {
        S3Uri s3Uri = new S3Uri("my-test-bucket", "path/to/file.json");

        assertEquals("s3://my-test-bucket/path/to/file.json", s3Uri.uri());
    }

    @Test
    public void testEmptyKey() {
        S3Uri s3Uri = new S3Uri("test-bucket", "");

        assertEquals("test-bucket", s3Uri.bucket());
        assertEquals("", s3Uri.key());
        assertEquals("s3://test-bucket/", s3Uri.uri());
    }

    @Test
    public void testPathWithLeadingSlash() {
        S3Uri s3Uri = new S3Uri("s3://mybucket//path/with/extra/slash");

        assertEquals("mybucket", s3Uri.bucket());
        assertEquals("/path/with/extra/slash", s3Uri.key());
        assertEquals("s3://mybucket//path/with/extra/slash", s3Uri.uri());
    }
}
