package com.here.xyz.util.service.aws;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Uri {
    private String bucket;
    private String key;
    private URI uri;
    private static final Pattern HTTPS_S3_PATTERN = Pattern.compile("https://([^.]+)\\.(s3[-.][^/]*\\.amazonaws\\.com)/(.*)");

    public S3Uri(URI uri) {
        this.uri = uri;
    }

    @JsonCreator
    public S3Uri(String uri) {
        this(URI.create(uri));
    }

    public S3Uri(String bucket, String key) {
        assert bucket != null;
        assert key != null;
        this.bucket = bucket;
        this.key = key;
    }

    public String bucket() {
        if (bucket == null) {
            String scheme = uri.getScheme();
            if ("s3".equals(scheme)) {
                bucket = uri.getHost();
            } else if ("https".equals(scheme)) {
                Matcher matcher = HTTPS_S3_PATTERN.matcher(uri.toString());
                if (matcher.matches()) {
                    bucket = matcher.group(1);
                }
            }
        }
        return bucket;
    }

    public String key() {
        if (key == null) {
            String scheme = uri.getScheme();
            if ("s3".equals(scheme)) {
                key = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
            } else if ("https".equals(scheme)) {
                Matcher matcher = HTTPS_S3_PATTERN.matcher(uri.toString());
                if (matcher.matches()) {
                    key = matcher.group(3);
                }
            }
        }
        return key;
    }

    public String uri() {
        if (uri == null) {
            uri = URI.create("s3://" + bucket + "/" + key);
        }
        return uri.toString();
    }

    @JsonValue
    @Override
    public String toString() {
        return uri().toString();
    }
}
