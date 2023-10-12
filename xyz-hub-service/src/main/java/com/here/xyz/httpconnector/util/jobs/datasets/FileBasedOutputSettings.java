package com.here.xyz.httpconnector.util.jobs.datasets;

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.TILEID_FC_B64;

import com.here.xyz.httpconnector.util.jobs.Job.CSVFormat;

public class FileBasedOutputSettings {
  private CSVFormat format = TILEID_FC_B64;
  private String partitionKey = "id";
  private int tileLevel = 12;
  private boolean clipped = false;
  private int maxTilesPerFile = 512;

  public CSVFormat getFormat() {
    return format;
  }

  public void setFormat(CSVFormat format) {
    this.format = format;
  }

  public FileBasedOutputSettings withFormat(CSVFormat format) {
    setFormat(format);
    return this;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public FileBasedOutputSettings withPartitionKey(String partitionKey) {
    setPartitionKey(partitionKey);
    return this;
  }

  public int getTileLevel() {
    return tileLevel;
  }

  public void setTileLevel(int tileLevel) {
    this.tileLevel = tileLevel;
  }

  public FileBasedOutputSettings withTileLevel(int tileLevel) {
    setTileLevel(tileLevel);
    return this;
  }

  public boolean isClipped() {
    return clipped;
  }

  public void setClipped(boolean clipped) {
    this.clipped = clipped;
  }

  public FileBasedOutputSettings withClipped(boolean clipped) {
    setClipped(clipped);
    return this;
  }

  public int getMaxTilesPerFile() {
    return maxTilesPerFile;
  }

  public void setMaxTilesPerFile(int maxTilesPerFile) {
    this.maxTilesPerFile = maxTilesPerFile;
  }

  public FileBasedOutputSettings withMaxTilesPerFile(int maxTilesPerFile) {
    setMaxTilesPerFile(maxTilesPerFile);
    return this;
  }
}
