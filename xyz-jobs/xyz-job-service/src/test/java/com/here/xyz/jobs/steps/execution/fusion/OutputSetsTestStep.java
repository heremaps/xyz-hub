package com.here.xyz.jobs.steps.execution.fusion;

import static com.here.xyz.jobs.steps.Step.Visibility.USER;

import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.pagination.Page;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputSetsTestStep extends TestStep<OutputSetsTestStep> {

  private final Map<String, Integer> countsPerSet = new HashMap<>();

  public OutputSetsTestStep(String jobId, String group, Map<String, Integer> countsPerSet) {
    this.setJobId(jobId);
    this.setOutputSetGroup(group);
    this.countsPerSet.putAll(countsPerSet);
    List<OutputSet> sets = new ArrayList<>();
    for (String setName : countsPerSet.keySet()) {
      sets.add(new OutputSet(setName, USER, false));
    }
    setOutputSets(sets);
  }

  private List<Output> dummyOutputs(int n) {
    List<Output> list = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      list.add(new DownloadUrl().withByteSize(0));
    }
    return list;
  }

  @Override
  public void execute(boolean resume) {
  }

  @Override
  public void cancel() {
  }

  @Override
  public boolean validate() {
    return true;
  }

  @Override
  public java.util.List<com.here.xyz.jobs.steps.resources.Load> getNeededResources() {
    return java.util.List.of();
  }

  @Override
  public int getTimeoutSeconds() {
    return 1;
  }

  @Override
  public int getEstimatedExecutionSeconds() {
    return 1;
  }

  @Override
  public List<Output> loadOutputs(Visibility visibility) {
    int sum = countsPerSet.values().stream().mapToInt(Integer::intValue).sum();
    return dummyOutputs(sum);
  }

  @Override
  public Page<Output> loadOutputsPage(Visibility visibility, String setName, int limit, String nextPageToken) {
    int n = countsPerSet.getOrDefault(setName, 0);
    return new Page<>(dummyOutputs(n), null);
  }

  @Override
  public String getDescription() {
    return "Fake step for testing";
  }
}
