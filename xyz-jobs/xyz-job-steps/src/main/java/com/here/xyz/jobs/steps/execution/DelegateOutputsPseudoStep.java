package com.here.xyz.jobs.steps.execution;

import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle;

import java.util.List;
/**
 * NOTE: This step implementation is a placeholder step, that will be used by the JobExecutor to inject outputs of a formerly run
 * job into the StepGraph of this job.
 * This step depicts a step of the formerly run job that was found to be a predecessor of the step at the border of the re-usable subgraph
 * of the formerly run StepGraph that was cut out because it matched a part of the new job's StepGraph.
 * This pseudo step creates a link to the new Job's StepGraph and the old step's outputs.
 * That way the succeeding Step(s) of the new StepGraph can access the outputs that have been produced by the old step.
 */

public class DelegateOutputsPseudoStep extends Step<DelegateOutputsPseudoStep> {
    @JsonView({Internal.class, Static.class})
    private String delegateJobId; //The old job ID
    @JsonView({Internal.class, Static.class})
    private String delegateStepId; //The old step ID

    DelegateOutputsPseudoStep(){}

    DelegateOutputsPseudoStep(String delegateJobId, String delegateStepId) {
        this.delegateJobId = delegateJobId;
        this.delegateStepId = delegateStepId;
    }

    @Override
    public String getJobId(){
        return delegateJobId;
    }

    @Override
    public String getId(){
        return delegateStepId;
    }

    @Override
    public RuntimeInfo getStatus(){
        return new RuntimeInfo().withState(RuntimeInfo.State.SUCCEEDED);
    }

    @Override
    public List<Load> getNeededResources() {
        return List.of();
    }

    @Override
    public int getTimeoutSeconds() {
        return 0;
    }

    @Override
    public int getEstimatedExecutionSeconds() {
        return 0;
    }

    @Override
    public String getDescription() {
        return "A pseudo step that just delegates outputs of a Step of another StepGraph into this StepGraph";
    }

    @Override
    public void execute() throws Exception {
        throw new IllegalAccessException("This method should never be called");
    }

    @Override
    public void resume() throws Exception {
        execute();
    }

    @Override
    public void cancel() throws Exception {}

    @Override
    public boolean validate() throws BaseHttpServerVerticle.ValidationException {
        return true;
    }
}
