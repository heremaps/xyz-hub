package com.here.xyz.jobs.steps.execution;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.here.xyz.XyzSerializable;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;

public class EcsTaskStep extends SyncExecutionStep<EcsTaskStep> {

  private static final Logger logger = LogManager.getLogger();

  private Map<String,Object> ecsTaskConfig;

  public void setEcsTaskConfig(Map<String, Object> ecsTaskConfig) {
    this.ecsTaskConfig = ecsTaskConfig;
  }

  private String additionalEcsInfo;

  public String getAdditionalEcsInfo() {
	return additionalEcsInfo;
  }

  public void setAdditionalEcsInfo(String additionalEcsInfo) {
	this.additionalEcsInfo = additionalEcsInfo;
  }

  public EcsTaskStep withAdditionalEcsInfo(String additionalEcsInfo) {
	setAdditionalEcsInfo( additionalEcsInfo );
    return this;
  }

  public Map<String,Object> getEcsTaskConfig( String payload )
  {
   if( ecsTaskConfig == null)
    ecsTaskConfig = Map.of( //kind of a placeholder
      "Cluster", "arn:aws:ecs:eu-west-1:609321179939:cluster/SIT",
      "TaskDefinition", "iml-job-steps:1",
      "LaunchType", "FARGATE",
/**/
      "NetworkConfiguration", Map.of(
         "awsvpcConfiguration", Map.of(
            "Subnets",List.of("subnet-00bfda51fadd1fb7a", "subnet-04fea06de2d5a8d12", "subnet-00628f3aff3d90fbd"),
            "SecurityGroups",List.of("sg-020c6cdcc7db7be32"),
            "AssignPublicIp","ENABLED"
            )
          )
/**/
    );

    ecsTaskConfig.put("Overrides", Map.of(
          "ContainerOverrides", List.of(
            Map.of(
              "Name", "job-step",
              "Environment", Map.of("Name","EcsTaskStep","Value",payload )
          ))
      ));

    logger.info(ecsTaskConfig);

    return ecsTaskConfig;
  }

    @Override
    public int getEstimatedExecutionSeconds() {
        return 60 * 60 * 6; // 6h
    }

    @Override
    public String getDescription() {
        return "stepfunction running as standalone task on ecs";
    }

    @Override
    public void execute(boolean resume) throws Exception {

     // if( Config.instance.LOCALSTACK_ENDPOINT != null ) {}
     logger.info("execute ->" + getAdditionalEcsInfo());

    }

    @Override
    public boolean validate() throws ValidationException {
    super.validate();

    return true;
  }

  public static void main(String[] args) {

   int i = 0;
   for (String arg : args) {
    logger.info( String.format( "%d - %s", i, arg ) );
   }


   String inputJson = System.getenv("EcsTaskStep");
   logger.info( String.format( "Env->[%s]", inputJson ) );

   try {

    LambdaBasedStep<?> lr = XyzSerializable.deserialize(inputJson, LambdaBasedStep.class);

    if( lr instanceof EcsTaskStep ecst )
    {
      ecst.execute(false);
      EcsTaskStep.logger.info("ecst.execute(false);");
    }
    else
    {
     EcsTaskStep.logger.info("--->>> " + lr.getClass().getCanonicalName() );
     throw new ValidationException(String.format("class %s must be instanceof EcsTaskStep",lr.getClass().getCanonicalName()));
    }

   } catch (Exception e) {
	   logger.error("", e);
     System.exit(1);
   }

  }

}
