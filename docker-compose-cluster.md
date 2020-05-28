# Running XYZ-HUB cluster - example for using WebMessageBroker

The WebMessageBroker provides an abstract MessageBroker implementation as an optional alternative to the default SnsMessageBroker implementation.

Currently one can choose between the following WebMessageBroker extends:

- StaticWebMessageBroker (*use e.g. for local development and testing*)
- S3WebMessageBroker (*use e.g. for a well known static set of xyz-hub instances*)
- TargetGroupWebMessageBroker (*use with AWS Elastic Loadbalancing*)
- ServiceDiscoveryWebMessageBroker (preferred, *use with AWS ECS Service Discovery*)

## Example local stack with docker-compose

An example stack and all dependencies can be started locally using Docker compose.

```bash
docker-compose -f docker-compose-cluster.yml build
```

```bash
docker-compose -f docker-compose-cluster.yml up -d --scale xyz-hub=2
```

```bash
Creating volume "xyz-hub_pgdata" with default driver
Creating volume "xyz-hub_pgadmindata" with default driver
Creating volume "xyz-hub_dynamodbdata" with default driver
Creating redis    ... done
Creating dynamodb        ... done
Creating postgres        ... done
Creating redis-commander   ... done
Creating pgadmin           ... done
Creating dynamodb-admin    ... done
Creating xyz-hub_xyz-hub_1 ... done
Creating xyz-hub_xyz-hub_2 ... done
Creating swagger-ui        ... done
Creating nginx             ... done
```

Now you can access the following endpoints.

- <http://localhost:8080/hub/>
- <http://localhost:8080/swagger/>
- <http://localhost:8080/pgadmin4/> (user@localhost/password)
- <http://localhost:8080/dynamodb/>
- <http://localhost:8080/redis/>

The example stack for the WebMessageBroker contains the below configuration for a default static target datasource.

```bash
ADMIN_MESSAGE_BROKER=StaticWebMessageBroker
STATIC_WEB_MESSAGE_BROKER_CONFIG='{"xyz-hub_xyz-hub_1": "8080","xyz-hub_xyz-hub_2": "8080"}'
```

### Example startup log output

```bash
docker-compose -f docker-compose-cluster.yml logs --tail=10000 --follow|grep MessageBroker
xyz-hub_2          | 2020-05-28 05:09:10,481 INFO  WebMessageBroker The StaticWebMessageBroker was initialized.
xyz-hub_2          | 2020-05-28 05:09:10,514 DEBUG WebMessageBroker Removing own instance (xyz-hub_xyz-hub_2:8080) from target endpoints.
xyz-hub_2          | 2020-05-28 05:09:10,514 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_1=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0
xyz-hub_1          | 2020-05-28 05:09:10,699 INFO  WebMessageBroker The StaticWebMessageBroker was initialized.
xyz-hub_1          | 2020-05-28 05:09:10,701 DEBUG WebMessageBroker Removing own instance (xyz-hub_xyz-hub_1:8080) from target endpoints.
xyz-hub_1          | 2020-05-28 05:09:10,702 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_2=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0
```

### Example publish log output

```bash
xyz-hub_1          | 2020-05-28 05:10:03,202 DEBUG WebMessageBroker Send AdminMessage.@Class: InvalidateSpaceCacheMessage , Source.Ip: 03cfdc8a1f50
xyz-hub_1          | 2020-05-28 05:10:03,206 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_2=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0
xyz-hub_1          | 2020-05-28 05:10:03,206 DEBUG WebMessageBroker Preparing request for target: xyz-hub_xyz-hub_2:8080
xyz-hub_1          | 2020-05-28 05:10:03,250 DEBUG WebMessageBroker Send AdminMessage to all target endpoints running in background.
xyz-hub_1          | 2020-05-28 05:10:03,250 DEBUG WebMessageBroker AdminMessage was: {"@class":"com.here.xyz.hub.config.SpaceConfigClient$InvalidateSpaceCacheMessage","source":{"id":"b7b657e8-1676-4684-9d4c-1935cf341687","ip":"03cfdc8a1f50","port":8080},"destination":null,"id":"aJinmelJ"}
xyz-hub_1          | 2020-05-28 05:10:03,250 DEBUG WebMessageBroker Drop AdminMessage.@Class: InvalidateSpaceCacheMessage , Source.Ip: 03cfdc8a1f50
xyz-hub_2          | 2020-05-28 05:10:03,361 DEBUG WebMessageBroker Handle AdminMessage.@Class: InvalidateSpaceCacheMessage , Source.Ip: 03cfdc8a1f50
```

## In Production

In production the WebMessageBroker should be best used with AWS ECS Service Discovery configuration or with AWS ELB Target Group configuration.

It is also possible to get a list of target endpoints from a json file object from S3.

### AWS ECS Service Discovery target datasource

```bash
ADMIN_MESSAGE_BROKER=ServiceDiscoveryWebMessageBroker
SERVICE_DISCOVERY_WEB_MESSAGE_BROKER_SERVICE_ID=srv-22dygtsduanhozc3
```

Assuming you are using AWS CloudFormation to deploy the xyz-hub as an ECS Service you may want to add a AWS::ServiceDiscovery::PrivateDnsNamespace and AWS::ServiceDiscovery::Service resources as well as a AWS::IAM::Policy allowing your instances to use the servicediscovery to your stack.

### AWS ELB Target Group target datasource

```bash
ADMIN_MESSAGE_BROKER=TargetGroupWebMessageBroker
TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN=arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/xyz-hub-service/12ab34cd56ef78gh
```

Assuming you are using AWS CloudFormation to deploy the xyz-hub as an ECS Service you may want to add a AWS::IAM::Policy allowing your instances to use the elasticloadbalancing to your stack.

### AWS S3 target datasource

```bash
ADMIN_MESSAGE_BROKER=S3WebMessageBroker
S3_WEB_MESSAGE_BROKER_BUCKET=xyz-hub-admin
S3_WEB_MESSAGE_BROKER_OBJECT=service-instances.json
```

Assuming you are using AWS CloudFormation to deploy the xyz-hub you may want to add a AWS::S3::Bucket to your stack.

## Configuration Reference

Note: Environment variables are preferred over properties!

### Available environment variables

```bash
# Enable the WebMessageBroker
ADMIN_MESSAGE_BROKER=<MessageBroker>

# Configure the WebMessageBroker periodic updates
# Note: when using the StaticWebMessageBroker periodic updates will be disabled
# Note: when using the ServiceDiscoveryWebMessageBroker you should enable periodic updates
# Note: when using the TargetGroupWebMessageBroker you should enable periodic updates
# Note: when using the S3WebMessageBroker you can enable periodic updates e.g. if your s3 object content is refreshed by some external process
WEB_MESSAGE_BROKER_PERIODIC_UPDATE=false
WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000

# Configure the StaticWebMessageBroker for static
ADMIN_MESSAGE_BROKER=StaticWebMessageBroker
STATIC_WEB_MESSAGE_BROKER_CONFIG='{"xyz-hub_xyz-hub_1": "8080","xyz-hub_xyz-hub_2": "8080"}'

# Configure the ServiceDiscoveryWebMessageBroker for AWS ECS Service Discovery
ADMIN_MESSAGE_BROKER=ServiceDiscoveryWebMessageBroker
SERVICE_DISCOVERY_WEB_MESSAGE_BROKER_SERVICE_ID=srv-22dygtsduanhozc3
WEB_MESSAGE_BROKER_PERIODIC_UPDATE=true
WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000

# Configure the TargetGroupWebMessageBroker for AWS ELB Target Group
ADMIN_MESSAGE_BROKER=TargetGroupWebMessageBroker
TARGET_GROUP_WEB_MESSAGE_BROKER_ELB_TARGETGROUP_ARN=arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/xyz-hub-service/12ab34cd56ef78gh
WEB_MESSAGE_BROKER_PERIODIC_UPDATE=true
WEB_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000

# Configure the S3WebMessageBroker for AWS S3
ADMIN_MESSAGE_BROKER=S3WebMessageBroker
S3_WEB_MESSAGE_BROKER_BUCKET=xyz-hub-admin
S3_WEB_MESSAGE_BROKER_OBJECT=service-instances.json
```

### Example json file object on S3

```json
{
    "xyz-hub_xyz-hub_1": "8080",
    "xyz-hub_xyz-hub_2": "8080"
}
```
