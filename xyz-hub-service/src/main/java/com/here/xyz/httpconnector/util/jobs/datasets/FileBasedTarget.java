package com.here.xyz.httpconnector.util.jobs.datasets;

public interface FileBasedTarget<T extends FileBasedTarget> {
  FileBasedOutputSettings getOutputSettings();
  void setOutputSettings(FileBasedOutputSettings outputSettings);
  T withOutputSettings(FileBasedOutputSettings outputSettings);
}
