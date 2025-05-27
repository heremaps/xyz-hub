package com.here.xyz.jobs.steps.execution;

import static com.here.xyz.jobs.steps.execution.InputSetReference.refFromInputSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.execution.resolver.EmrScriptResolver;
import com.here.xyz.jobs.steps.execution.resolver.S3Resolver;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

class RunEmrJobTest {

  private static final Logger logger = LogManager.getLogger();
  private static final String WHITESPACE = "\\s+";

  @Test
  public void testS3UriReplacement() {
    String scriptParamsString = "s3://test-bucket/outputs/inputData,s3://test-bucket/outputs/contextData s3://test-bucket/outputData/ --pidFormat=quadkey --inputFormat=jsonwkb --tileFormat=here --type=densityPartitionv2 --contextDir=s3://test-bucket/outputs/contextData";
    List<String> scriptParams = Arrays.stream(scriptParamsString.split(WHITESPACE)).toList();

    logScriptParams("BEFORE", scriptParams);

    assertTrue(String.join("\n", scriptParams).contains("s3://"));
    scriptParams = new S3Resolver(getTmpDir()).resolveScriptParams(scriptParams);

    logScriptParams("AFTER", scriptParams);

    assertFalse(String.join("\n", scriptParams).contains("s3://"));
  }

  @Test
  public void testFullReplacement() {
    Config.instance = new Config();
    Config.instance.JOBS_S3_BUCKET = "dummybucket";

    InputSet mpaInput = new InputSet("jobIdA", "providerIdA", "input", true);
    InputSet mpaContext = new InputSet("jobIdB", "providerIdB", "context", true);
    logger.info(mpaInput.toString());
    Collection<InputSet> inputSets = List.of(mpaInput, mpaContext);

    String scriptParamsString = refFromInputSet(mpaInput) + "," + refFromInputSet(mpaContext)
        + " s3://test-bucket/mpa/outputDir/ --pidFormat=quadkey --inputFormat=jsonwkb --tileFormat=here --type=densityPartitionv2 --contextDir="
        + refFromInputSet(mpaContext);
    List<String> scriptParams = Arrays.stream(scriptParamsString.split("\\s+")).toList();

    logScriptParams("BEFORE", scriptParams);

    assertTrue(String.join("\n", scriptParams).contains("s3://"));
    EmrScriptResolver resolver = new EmrScriptResolver("myJobId", "myStepId", getTmpDir(), inputSets);
    scriptParams = resolver.resolveScriptParams(scriptParams);

    logScriptParams("AFTER", scriptParams);

    assertFalse(String.join("\n", scriptParams).contains("s3://"));
    assertEquals("/tmp/dummybucket/jobIdA/providerIdA/outputs/input,/tmp/dummybucket/jobIdB/providerIdB/outputs/context",
        scriptParams.get(0));
    assertEquals("/tmp/test-bucket/mpa/outputDir/", scriptParams.get(1));
    assertEquals("--contextDir=/tmp/dummybucket/jobIdB/providerIdB/outputs/context",
        scriptParams.stream().filter(p -> p.startsWith("--contextDir=")).findFirst().get());
  }

  private void logScriptParams(String marker, List<String> scriptParams) {
    logger.info("{}:\n\t{}", marker, String.join("\n\t", scriptParams));
  }

  private String getTmpDir() {
    return String.format("/tmp/%s/", UUID.randomUUID().toString().substring(0, 4));
  }

}
