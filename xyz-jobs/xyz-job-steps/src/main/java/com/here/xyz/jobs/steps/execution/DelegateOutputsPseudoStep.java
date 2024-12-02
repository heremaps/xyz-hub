package com.here.xyz.jobs.steps.execution;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.util.S3Client;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * NOTE: This step implementation is a placeholder step, that will be used by the JobExecutor to inject outputs of a formerly run
 * job into the StepGraph of this job.
 * This step depicts a step of the formerly run job that was found to be a predecessor of the step at the border of the re-usable subgraph
 * of the formerly run StepGraph that was cut out because it matched a part of the new job's StepGraph.
 * This pseudo step creates a link to the new Job's StepGraph and the old step's outputs.
 * That way the succeeding Step(s) of the new StepGraph can access the outputs that have been produced by the old step.
 */

public class DelegateOutputsPseudoStep extends Step<DelegateOutputsPseudoStep> {
    private static final Logger logger = LogManager.getLogger();

    @JsonView({Internal.class, Static.class})
    private String delegateJobId; //The old job ID which gets reused
    @JsonView({Internal.class, Static.class})
    private String delegateStepId; //The old step ID which gets reused
    @JsonView({Internal.class, Static.class})
    private String originalJobId; //The current job ID
    @JsonView({Internal.class, Static.class})
    private String originalStepId; //The current job ID

    DelegateOutputsPseudoStep(){}

    DelegateOutputsPseudoStep(String delegateJobId, String delegateStepId, String originalJobId, String originalStepId) {
        this.delegateJobId = delegateJobId;
        this.delegateStepId = delegateStepId;
        this.originalJobId = originalJobId;
        this.originalStepId = originalStepId;

        addJobReferenceToDelegatedJob(delegateJobId, originalJobId);
    }

    public String getDelegateJobId() {
        return delegateJobId;
    }

    public String getDelegateStepId() {
        return delegateStepId;
    }

    @Override
    public String getJobId(){
        return delegateJobId;
    }

    @Override
    public String getId(){
        return delegateStepId;
    }

    public String getOriginalJobId(){
        return originalJobId;
    }

    public String getOriginalStepId(){
        return originalStepId;
    }

    //first old path / second new path
    @JsonIgnore
    public String[] getReplacementPathForFiles(){
        return new String[]{ originalJobId +"/"+ originalStepId , delegateJobId +"/"+ delegateStepId };
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

    @Override
    public void deleteOutputs(){
        //DelegateOutputs Steps are not permitted to delete outputs
    }

    public static String referenceMetaS3Key(String jobId) {
        return jobId + "/meta/referencing_jobs.json";
    }

    public static void saveReferenceMetadata(String jobId, ReferenceMetadata metadata) throws IOException {
        if (metadata != null) {
            S3Client.getInstance().putObject(referenceMetaS3Key(jobId), "application/json", metadata.serialize());
        }
    }

    public static ReferenceMetadata loadReferenceMetadata(String delegateJobId) throws IOException {
        try {
            return XyzSerializable.deserialize(S3Client.getInstance().loadObjectContent(referenceMetaS3Key(delegateJobId)),
                    ReferenceMetadata.class);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return ReferenceMetadata.empty();
            }
            throw e;
        }
    }

    private static void addJobReferenceToDelegatedJob(String delegateJobId, String jobIdToReference) {
        try {
            ReferenceMetadata metadata = loadReferenceMetadata(delegateJobId);

            if(metadata == null)
                metadata = new ReferenceMetadata(Set.of(jobIdToReference));
            else
                metadata.referencingJobs().add(jobIdToReference);

            saveReferenceMetadata(delegateJobId, metadata);
        } catch (IOException e) {
            logger.error("Error handling ReferenceMetadata for job {}.", delegateJobId, e);
            throw new RuntimeException("Not able to add reference to delegate job!");
        }
    }

    public record ReferenceMetadata(@JsonProperty Set<String> referencingJobs) implements XyzSerializable {
        // Static factory method for an "empty constructor"
        public static ReferenceMetadata empty() {
            return new ReferenceMetadata(new HashSet<>());
        }
    }
}
