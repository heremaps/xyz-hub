package com.here.xyz.httpconnector.util.jobs.datasets;

public class Files<T extends Files> extends DatasetDescription implements FileBasedTarget<T> {

  FileBasedOutputSettings outputSettings = new FileBasedOutputSettings();

  public FileBasedOutputSettings getOutputSettings() {
    return outputSettings;
  }

  public void setOutputSettings(FileBasedOutputSettings outputSettings) {
    this.outputSettings = outputSettings;
  }

  public T withOutputSettings(FileBasedOutputSettings outputSettings) {
    setOutputSettings(outputSettings);
    return (T) this;
  }

  @Override
  public String getKey() {
    //No specific key to search for.
    return null;
  }
}
