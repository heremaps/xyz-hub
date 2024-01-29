package com.here.xyz.httpconnector.util.jobs.datasets;

import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.GEOJSON;
import static com.here.xyz.httpconnector.util.jobs.Job.CSVFormat.JSON_WKB;

import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Csv;
import com.here.xyz.httpconnector.util.jobs.datasets.files.FileChunking;
import com.here.xyz.httpconnector.util.jobs.datasets.files.FileFormat;
import com.here.xyz.httpconnector.util.jobs.datasets.files.GeoJson;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning;
import com.here.xyz.httpconnector.util.jobs.datasets.files.Partitioning.FeatureKey;
import java.util.Map;

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

  public void setFormat(Object format) {
    //TODO: Remove BWC hack after refactoring
    if(format instanceof  FileFormat fileFormat)
      this.format = fileFormat;
    else if(format instanceof Map map && map.containsKey("type"))
      this.format = XyzSerializable.fromMap(map, FileFormat.class);
    else if (format instanceof String formatString) {
      if (GEOJSON.toString().equals(formatString))
        this.format = new GeoJson();
      else if (JSON_WKB.toString().equals(formatString))
        this.format = new Csv();
      else
        this.format = new Csv().withAddPartitionKey(true);
    }
    else
      this.format = new Csv();
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
