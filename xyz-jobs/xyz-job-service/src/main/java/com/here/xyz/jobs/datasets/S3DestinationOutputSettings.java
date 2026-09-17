package com.here.xyz.jobs.datasets;

public class S3DestinationOutputSettings extends FileOutputSettings {
  private String bucket;
  private String prefix;

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public S3DestinationOutputSettings withBucket(String bucket) {
    setBucket(bucket);
    return this;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    if (prefix.startsWith("/"))
      prefix = prefix.substring(1);
    this.prefix = prefix;
  }

  public S3DestinationOutputSettings withPrefix(String prefix) {
    setPrefix(prefix);
    return this;
  }

}
