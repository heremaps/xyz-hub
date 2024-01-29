package com.here.xyz.httpconnector.util.jobs.datasets;

import com.here.xyz.httpconnector.util.jobs.datasets.files.FileInputSettings;

public class Files<T extends Files> extends DatasetDescription implements FileBasedTarget<T> {
  FileOutputSettings outputSettings = new FileOutputSettings();
  FileInputSettings inputSettings = new FileInputSettings();

  public FileOutputSettings getOutputSettings() {
    return outputSettings;
  }

  public void setOutputSettings(FileOutputSettings outputSettings) {
    this.outputSettings = outputSettings;
  }

  public T withOutputSettings(FileOutputSettings outputSettings) {
    setOutputSettings(outputSettings);
    return (T) this;
  }

  public FileInputSettings getInputSettings() {
    return inputSettings;
  }

  public void setInputSettings(FileInputSettings inputSettings) {
    this.inputSettings = inputSettings;
  }

  public T withInputSettings(FileInputSettings inputSettings) {
    setInputSettings(inputSettings);
    return (T) this;
  }

  @Override
  public String getKey() {
    //No specific key to search for.
    return null;
  }
}
