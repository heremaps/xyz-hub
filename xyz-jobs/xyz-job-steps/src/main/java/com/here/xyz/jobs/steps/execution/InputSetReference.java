package com.here.xyz.jobs.steps.execution;

import com.here.xyz.jobs.steps.Step.InputSet;
import java.util.regex.Pattern;

public class InputSetReference {

  public static final String INPUT_SET_REF_PREFIX = "${inputSet:";
  public static final String INPUT_SET_REF_SUFFIX = "}";
  public static final Pattern INPUT_SET_REF_PATTERN = Pattern.compile(
      Pattern.quote(INPUT_SET_REF_PREFIX) + "([a-zA-Z0-9._=-]+)" + Pattern.quote(INPUT_SET_REF_SUFFIX));

  private final String encoded;

  private InputSetReference(String encodedReference) {
    this.encoded = encodedReference;
  }

  public static InputSetReference refFromInputSet(InputSet inputSet) {
    return new InputSetReference(INPUT_SET_REF_PREFIX + toReferenceIdentifier(inputSet) + INPUT_SET_REF_SUFFIX);
  }

  private static String toReferenceIdentifier(InputSet inputSet) {
    return inputSet.providerId() + "." + inputSet.name();
  }

  @Override
  public String toString() {
    return encoded;
  }

}
