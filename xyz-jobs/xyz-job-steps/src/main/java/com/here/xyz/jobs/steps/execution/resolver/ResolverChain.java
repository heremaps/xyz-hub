package com.here.xyz.jobs.steps.execution.resolver;

import java.util.ArrayList;
import java.util.List;

public class ResolverChain implements ScriptTokenResolver {

  private final List<ScriptTokenResolver> resolvers;

  public ResolverChain(List<ScriptTokenResolver> orderedListOfResolvers) {
    this.resolvers = orderedListOfResolvers;
  }

  public ResolverChain(ScriptTokenResolver... orderedListOfResolvers) {
    this(List.of(orderedListOfResolvers));
  }

  @Override
  public List<String> resolveScriptParams(List<String> scriptParams) {
    final List<String>[] resolved = new List[]{new ArrayList<>()};
    resolved[0].addAll(scriptParams);
    resolvers.forEach(r -> {
      resolved[0] = r.resolveScriptParams(resolved[0]);
    });
    return resolved[0];
  }
}
