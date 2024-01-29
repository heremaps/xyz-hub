package com.here.xyz.httpconnector.util.jobs.datasets.files;

public class FileChunking {
  private int maxSizeMB = 512;

  public int getMaxSizeMB() {
    return maxSizeMB;
  }

  public void setMaxSizeMB(int maxSizeMB) {
    this.maxSizeMB = maxSizeMB;
  }

  public FileChunking withMaxSizeMB(int maxSizeMB) {
    setMaxSizeMB(maxSizeMB);
    return this;
  }
}
