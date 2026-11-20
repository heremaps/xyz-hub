package com.here.xyz.jobs.steps.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;

public class EcsTaskStep extends SyncLambdaStep<EcsTaskStep> {

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
    ecsTaskConfig = defaultEcsTaskConfig();

    ecsTaskConfig = new HashMap<String,Object>(ecsTaskConfig); // orig possibly immutable

    ecsTaskConfig.put("Overrides", Map.of(
          "ContainerOverrides", List.of(
            Map.of(
              "Name", requiredSetting("ECS_CONTAINER_NAME", Config.instance.ECS_CONTAINER_NAME),
              "Environment", List.of(Map.of("Name","EcsTaskStep","Value",payload ))
          ))
      ));

    logger.info(ecsTaskConfig);

    return ecsTaskConfig;
  }

  /**
   * Builds the RunTask parameters from the service configuration (see the ECS_* settings in {@link Config}).
   * The deployment (e.g. MapFlow) provides the actual values.
   */
  private static Map<String, Object> defaultEcsTaskConfig() {
    Config config = Config.instance;
    return Map.of(
        "Cluster", requiredSetting("ECS_CLUSTER_ARN", config.ECS_CLUSTER_ARN),
        "TaskDefinition", requiredSetting("ECS_TASK_DEFINITION", config.ECS_TASK_DEFINITION),
        "LaunchType", "FARGATE",
        "NetworkConfiguration", Map.of(
            "AwsvpcConfiguration", Map.of(
                "Subnets", requiredList("ECS_SUBNETS", config.ecsSubnets()),
                "SecurityGroups", requiredList("ECS_SECURITY_GROUPS", config.ecsSecurityGroups()),
                "AssignPublicIp", config.ECS_ASSIGN_PUBLIC_IP != null ? config.ECS_ASSIGN_PUBLIC_IP : "DISABLED"
            )
        )
    );
  }

  /**
   * Checks that the service configuration has all settings the default RunTask parameters need.
   * @throws IllegalStateException naming the first missing setting
   */
  public static void checkConfiguration() {
    defaultEcsTaskConfig();
    requiredSetting("ECS_CONTAINER_NAME", Config.instance.ECS_CONTAINER_NAME);
  }

  private static String requiredSetting(String name, String value) {
    if (value == null || value.isBlank())
      throw new IllegalStateException("The setting " + name + " is required to run an " + EcsTaskStep.class.getSimpleName() + ".");
    return value;
  }

  private static List<String> requiredList(String name, List<String> values) {
    if (values.isEmpty())
      throw new IllegalStateException("The setting " + name + " is required to run an " + EcsTaskStep.class.getSimpleName() + ".");
    return values;
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
    //Initialize Config from environment variables, as the step lambda does (LambdaBasedStepExecutor). The steps read it on construction.
    if (Config.instance == null) {
      XyzSerializable.fromMap(Map.copyOf(System.getenv()), Config.class);
      LambdaBasedStepExecutor.loadPlugins();
    }

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
