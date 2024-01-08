package com.here.xyz.httpconnector.util.emr.config;

import com.here.xyz.XyzSerializable;
import java.util.List;

public class EmrConfig implements XyzSerializable {
  private List<Step> steps;

  public List<Step> getSteps() {
    return steps;
  }

  public void setSteps(List<Step> steps) {
    this.steps = steps;
  }

  public EmrConfig withSteps(List<Step> steps) {
    setSteps(steps);
    return this;
  }
}
