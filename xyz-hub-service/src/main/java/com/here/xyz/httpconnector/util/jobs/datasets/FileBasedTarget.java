package com.here.xyz.httpconnector.util.jobs.datasets;

public interface FileBasedTarget<T extends FileBasedTarget> {
  FileOutputSettings getOutputSettings();
  void setOutputSettings(FileOutputSettings outputSettings);
  T withOutputSettings(FileOutputSettings outputSettings);
}
