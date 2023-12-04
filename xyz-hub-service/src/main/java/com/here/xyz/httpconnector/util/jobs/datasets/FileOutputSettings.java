package com.here.xyz.httpconnector.util.jobs.datasets;

import com.here.xyz.httpconnector.util.jobs.datasets.files.FileChunking;
import com.here.xyz.httpconnector.util.jobs.datasets.files.FileFormat;
import com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning.FeatureKey;

public class FileOutputSettings {
  private FileFormat format = new GeoJson();
  private Partitioning partitioning = new FeatureKey();
  private FileChunking chunking = new FileChunking();

  //Legacy fields:
  private String partitionKey = "tileid";
  private int tileLevel = 12;
  private boolean clipped = false;
  private int maxTilesPerFile = 512;

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(FileFormat format) {
    this.format = format;
  }

  public FileOutputSettings withFormat(FileFormat format) {
    setFormat(format);
    return this;
  }

  public Partitioning getPartitioning() {
    return partitioning;
  }

  public void setPartitioning(Partitioning partitioning) {
    this.partitioning = partitioning;
  }

  public FileOutputSettings withPartitioning(Partitioning partitioning) {
    setPartitioning(partitioning);
    return this;
  }

  public FileChunking getChunking() {
    return chunking;
  }

  public void setChunking(FileChunking chunking) {
    this.chunking = chunking;
  }

  public FileOutputSettings withChunking(FileChunking chunking) {
    setChunking(chunking);
    return this;
  }

  //Legacy getters & setters:
  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public FileOutputSettings withPartitionKey(String partitionKey) {
    setPartitionKey(partitionKey);
    return this;
  }

  public int getTileLevel() {
    return tileLevel;
  }

  public void setTileLevel(int tileLevel) {
    this.tileLevel = tileLevel;
  }

  public FileOutputSettings withTileLevel(int tileLevel) {
    setTileLevel(tileLevel);
    return this;
  }

  public boolean isClipped() {
    return clipped;
  }

  public void setClipped(boolean clipped) {
    this.clipped = clipped;
  }

  public FileOutputSettings withClipped(boolean clipped) {
    setClipped(clipped);
    return this;
  }

  public int getMaxTilesPerFile() {
    return maxTilesPerFile;
  }

  public void setMaxTilesPerFile(int maxTilesPerFile) {
    this.maxTilesPerFile = maxTilesPerFile;
  }

  public FileOutputSettings withMaxTilesPerFile(int maxTilesPerFile) {
    setMaxTilesPerFile(maxTilesPerFile);
    return this;
  }
}
