package com.here.xyz.jobs.steps.execution.resolver;

import com.here.xyz.jobs.steps.Step.InputSet;
import java.util.Collection;
import java.util.List;

public class EmrScriptResolver implements ScriptTokenResolver {

  private final InputSetResolver inputSetResolver;
  private final S3Resolver s3Inputs;
  private final S3Resolver s3Outputs;

  public EmrScriptResolver(String jobId, String stepId, String tmpDir, Collection<InputSet> inputSets) {
    inputSetResolver = new InputSetResolver(jobId, stepId, inputSets);
    s3Inputs = new S3Resolver(tmpDir);
    s3Outputs = new S3Resolver(tmpDir);
  }

  @Override
  public List<String> resolveScriptParams(List<String> rawScriptParams) {
    List<String> resolved = List.copyOf(rawScriptParams);
    resolved = new ResolverChain(inputSetResolver, s3Inputs).resolveScriptParams(resolved);
    resolved = s3Outputs.resolveScriptParams(resolved);
    return resolved;
  }

  public void prepareInputDirectories() {
    s3Outputs.createLocalDirectories();
    s3Inputs.downloadFromS3();
  }

  public void publishOutputDirectories() {
    s3Outputs.uploadLocalFilesToS3();
  }
}
