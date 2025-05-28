package com.here.xyz.jobs.steps.execution.resolver;

import static java.util.regex.Matcher.quoteReplacement;

import com.here.xyz.jobs.steps.Step.InputSet;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

public class InputSetResolver implements ScriptTokenResolver {

  private static final String INPUT_SET_REF_PREFIX = "${inputSet:";
  private static final String INPUT_SET_REF_SUFFIX = "}";
  private static final Pattern INPUT_SET_REF_PATTERN = Pattern.compile(
      Pattern.quote(INPUT_SET_REF_PREFIX) + "([a-zA-Z0-9._=-]+)" + Pattern.quote(INPUT_SET_REF_SUFFIX));
  private final String jobId;
  private final String stepId;
  private final Collection<InputSet> inputSets;


  public InputSetResolver(String jobId, String stepId, Collection<InputSet> inputSets) {
    this.jobId = jobId;
    this.stepId = stepId;
    this.inputSets = inputSets;
  }

  public static String mapInputReferencesIn(String scriptParam, Function<String, String> mapper) {
    if (scriptParam == null) {
      return null;
    }
    return INPUT_SET_REF_PATTERN.matcher(scriptParam)
        .replaceAll(match -> {
          String replacement = mapper.apply(match.group(1));
          if (replacement == null) {
            return quoteReplacement(match.group());
          }
          return quoteReplacement(replacement);
        });
  }

  public static String toInputSetReference(InputSet inputSet) {
    return INPUT_SET_REF_PREFIX + toReferenceIdentifier(inputSet) + INPUT_SET_REF_SUFFIX;
  }

  private static String toReferenceIdentifier(InputSet inputSet) {
    return inputSet.providerId() + "." + inputSet.name();
  }

  @Override
  public List<String> resolveScriptParams(List<String> scriptParams) {
    return scriptParams.stream().map(this::resolveScriptParam).toList();
  }

  private String resolveScriptParam(String scriptParam) {
    return mapInputReferencesIn(scriptParam,
        referenceIdentifier -> fromReferenceIdentifier(referenceIdentifier).toS3Uri(jobId).toString());
  }

  private InputSet fromReferenceIdentifier(String referenceIdentifier) {
    ReferenceIdentifier ref = ReferenceIdentifier.fromString(referenceIdentifier);
    return getInputSet(ref.stepId(), ref.name());
  }

  public Collection<InputSet> getInputSets() {
    return inputSets;
  }

  public String getStepId() {
    return stepId;
  }

  public String inputSetReference(InputSet inputSet) {
    if (!getInputSets().contains(inputSet)) {
      throw new IllegalArgumentException("The provided inputSet is not a part of this EMR step.");
    }

    return toInputSetReference(inputSet);
  }

  protected InputSet getInputSet(String providerId, String name) {
    try {
      return getInputSets().stream()
          .filter(inputSet -> Objects.equals(inputSet.name(), name) && Objects.equals(inputSet.providerId(), providerId))
          .findFirst()
          .get();
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException("No input set \"" + providerId + "." + name + "\" exists in step \"" + getStepId() + "\"");
    }
  }

  record ReferenceIdentifier(String stepId, String name) {

    public static ReferenceIdentifier fromString(String referenceIdentifier) {
      return new ReferenceIdentifier(referenceIdentifier.substring(0, referenceIdentifier.indexOf(".")),
          referenceIdentifier.substring(referenceIdentifier.indexOf(".") + 1));
    }
  }

}
