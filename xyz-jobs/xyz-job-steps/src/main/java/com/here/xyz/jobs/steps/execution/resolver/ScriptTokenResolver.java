package com.here.xyz.jobs.steps.execution.resolver;

import java.util.List;

public interface ScriptTokenResolver {

  List<String> resolveScriptParams(List<String> scriptParams);
}
