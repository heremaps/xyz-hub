package com.here.xyz.httpconnector.util.jobs.datasets.files;

public class FileInputSettings {
  private FileFormat format = new GeoJson();

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(FileFormat format) {
    this.format = format;
  }

  public FileInputSettings withFormat(FileFormat format) {
    setFormat(format);
    return this;
  }
}
