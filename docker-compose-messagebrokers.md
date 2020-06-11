# Running XYZ-HUB cluster - example for using MessageBroker

Currently one can choose between the following MessageBroker implementations:

- RedisMessageBroker (*default implementation, use with Redis*)
- S3WebMessageBroker (*use with AWS S3 object*)
- ServiceDiscoveryWebMessageBroker (*use with AWS ECS Service Discovery ID*)
- SnsMessageBroker (*use with AWS SNS topic subscription and a public messaging endpoint*)
- StaticWebMessageBroker (*use for local development and testing*)
- TargetGroupWebMessageBroker (*use with AWS Elastic Loadbalancing target group*)

## Example local stack with docker-compose

An example stack and all dependencies can be started locally using Docker compose.

```bash
docker-compose -f docker-compose-messagebrokers.yml build
```

```bash
docker-compose -f docker-compose-messagebrokers.yml up -d --scale xyz-hub=3
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
Creating xyz-hub_xyz-hub_3 ... done
Creating swagger-ui        ... done
Creating nginx             ... done
```

Now you can access the following endpoints.

- <http://localhost:8080/hub/>
- <http://localhost:8080/swagger/>
- <http://localhost:8080/pgadmin4/> (user@localhost/password)
- <http://localhost:8080/dynamodb/>
- <http://localhost:8080/redis/>

The example stack for the MessageBroker contains the below configuration for a StaticWebMessageBroker.

```bash
ADMIN_MESSAGE_BROKER=StaticWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"xyz-hub_xyz-hub_1": "8080","xyz-hub_xyz-hub_2": "8080","xyz-hub_xyz-hub_3": "8080"}'
```

## Running the different MessageBroker configurations

### RedisMessageBroker

```bash
ADMIN_MESSAGE_BROKER=RedisMessageBroker
```

#### Example startup debug log output

```log
docker-compose -f docker-compose-messagebrokers.yml logs --tail=10000 --follow|grep MessageBroker
xyz-hub_2          | 2020-06-11 19:58:24,206 INFO  RedisMessageBroker Subscribing the NODE=http://ab169c764655:8080 
xyz-hub_3          | 2020-06-11 19:58:24,543 INFO  RedisMessageBroker Subscribing the NODE=http://f5bbdeba59e9:8080 
xyz-hub_1          | 2020-06-11 19:58:24,626 INFO  RedisMessageBroker Subscribing the NODE=http://080d63f68e8c:8080 
xyz-hub_2          | 2020-06-11 19:58:24,952 INFO  RedisMessageBroker Subscription succeeded for NODE=http://ab169c764655:8080 
xyz-hub_3          | 2020-06-11 19:58:25,154 INFO  RedisMessageBroker Subscription succeeded for NODE=http://f5bbdeba59e9:8080 
xyz-hub_1          | 2020-06-11 19:58:25,250 INFO  RedisMessageBroker Subscription succeeded for NODE=http://080d63f68e8c:8080
```

#### Example publish debug log output

```log
xyz-hub_1          | 2020-06-11 19:58:30,531 DEBUG RedisMessageBroker Message has been sent with following content: {"@class":"com.here.xyz.hub.config.SpaceConfigClient$InvalidateSpaceCacheMessage","source":{"id":"0f468ee4-0748-4415-923f-6df99cd0ba17","ip":"080d63f68e8c","port":8080},"destination":null,"id":"4MzuXzHR"}
```

### S3WebMessageBroker

```bash
ADMIN_MESSAGE_BROKER=S3WebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"s3Uri": "s3://xyz-hub-admin/service-instances.json"}'
```

### ServiceDiscoveryWebMessageBroker

```bash
ADMIN_MESSAGE_BROKER=ServiceDiscoveryWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"serviceDiscoveryId": "xyz-hub"}'
```

### SnsMessageBroker

```bash
ADMIN_MESSAGE_BROKER=SnsMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"snsTopicArn": "arn:aws:sns:us-east-2:123456789012:xyz-hub-admin-messages"}'
```

### StaticWebMessageBroker

```bash
ADMIN_MESSAGE_BROKER=StaticWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"xyz-hub_1": "8080","xyz-hub_2": "8080"}'
```

#### Example startup debug log output

```log
docker-compose -f docker-compose-messagebrokers.yml logs --tail=10000 --follow|grep MessageBroker
xyz-hub_3          | 2020-06-11 19:55:41,813 INFO  WebMessageBroker The StaticWebMessageBroker was initialized. 
xyz-hub_3          | 2020-06-11 19:55:41,822 DEBUG WebMessageBroker Removing own instance (xyz-hub_xyz-hub_3:8080) from target endpoints. 
xyz-hub_3          | 2020-06-11 19:55:41,823 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_1=8080, xyz-hub_xyz-hub_2=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0 
xyz-hub_3          | 2020-06-11 19:55:41,823 DEBUG WebMessageBroker ADMIN_MESSAGE_BROKER_CONFIG: {xyz-hub_xyz-hub_1=8080, xyz-hub_xyz-hub_2=8080} 
xyz-hub_2          | 2020-06-11 19:55:42,208 INFO  WebMessageBroker The StaticWebMessageBroker was initialized. 
xyz-hub_2          | 2020-06-11 19:55:42,305 DEBUG WebMessageBroker Removing own instance (xyz-hub_xyz-hub_2:8080) from target endpoints. 
xyz-hub_2          | 2020-06-11 19:55:42,305 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_1=8080, xyz-hub_xyz-hub_3=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0 
xyz-hub_2          | 2020-06-11 19:55:42,305 DEBUG WebMessageBroker ADMIN_MESSAGE_BROKER_CONFIG: {xyz-hub_xyz-hub_1=8080, xyz-hub_xyz-hub_3=8080} 
xyz-hub_1          | 2020-06-11 19:55:42,981 INFO  WebMessageBroker The StaticWebMessageBroker was initialized. 
xyz-hub_1          | 2020-06-11 19:55:42,985 DEBUG WebMessageBroker Removing own instance (xyz-hub_xyz-hub_1:8080) from target endpoints. 
xyz-hub_1          | 2020-06-11 19:55:42,986 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_2=8080, xyz-hub_xyz-hub_3=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0 
xyz-hub_1          | 2020-06-11 19:55:42,986 DEBUG WebMessageBroker ADMIN_MESSAGE_BROKER_CONFIG: {xyz-hub_xyz-hub_2=8080, xyz-hub_xyz-hub_3=8080}
```

#### Example publish debug log output

```log
xyz-hub_3          | 2020-06-11 19:55:52,553 DEBUG WebMessageBroker TARGET_ENDPOINTS: {xyz-hub_xyz-hub_1=8080, xyz-hub_xyz-hub_2=8080}, PeriodicUpdate: false, PeriodicUpdateDelay: 0 
xyz-hub_3          | 2020-06-11 19:55:52,553 DEBUG WebMessageBroker Preparing request for target: xyz-hub_xyz-hub_1:8080 
xyz-hub_3          | 2020-06-11 19:55:52,571 DEBUG WebMessageBroker Preparing request for target: xyz-hub_xyz-hub_2:8080 
xyz-hub_3          | 2020-06-11 19:55:52,573 DEBUG WebMessageBroker Send AdminMessage to all target endpoints running in background. 
xyz-hub_3          | 2020-06-11 19:55:52,573 DEBUG WebMessageBroker AdminMessage has been sent with following content: {"@class":"com.here.xyz.hub.config.SpaceConfigClient$InvalidateSpaceCacheMessage","source":{"id":"fc724d7e-9c36-4398-a1b4-546be57895eb","ip":"a4d592a0d32f","port":8080},"destination":null,"id":"9JgLXEsF"} 
xyz-hub_2          | 2020-06-11 19:55:52,744 DEBUG WebMessageBroker AdminMessage has been received with following content: "{\"@class\":\"com.here.xyz.hub.config.SpaceConfigClient$InvalidateSpaceCacheMessage\",\"source\":{\"id\":\"fc724d7e-9c36-4398-a1b4-546be57895eb\",\"ip\":\"a4d592a0d32f\",\"port\":8080},\"destination\":null,\"id\":\"9JgLXEsF\"}" 
xyz-hub_1          | 2020-06-11 19:55:52,773 DEBUG WebMessageBroker AdminMessage has been received with following content: "{\"@class\":\"com.here.xyz.hub.config.SpaceConfigClient$InvalidateSpaceCacheMessage\",\"source\":{\"id\":\"fc724d7e-9c36-4398-a1b4-546be57895eb\",\"ip\":\"a4d592a0d32f\",\"port\":8080},\"destination\":null,\"id\":\"9JgLXEsF\"}"
```

### TargetGroupWebMessageBroker

```bash
ADMIN_MESSAGE_BROKER=TargetGroupWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"targetGroupArn": "arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/xyz-hub/73e2d6bc24d8a067"}'
```

## Configuration Reference

```bash
# Set the MessageBroker
ADMIN_MESSAGE_BROKER=<MessageBroker>

# Configure the WebMessageBrokers periodic updates
# Note: when using the S3WebMessageBroker you can enable periodic updates e.g. if your s3 object content is refreshed by some external process
# Note: when using the ServiceDiscoveryWebMessageBroker you should enable periodic updates
# Note: when using the StaticWebMessageBroker periodic updates will be disabled
# Note: when using the TargetGroupWebMessageBroker you should enable periodic updates
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE=false
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000

# Configure the S3WebMessageBroker for AWS S3
ADMIN_MESSAGE_BROKER=S3WebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"s3Uri": "s3://xyz-hub-admin/service-instances.json"}'

# Configure the ServiceDiscoveryWebMessageBroker for AWS ECS Service Discovery
ADMIN_MESSAGE_BROKER=ServiceDiscoveryWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"serviceDiscoveryId": "xyz-hub"}'
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE=true
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000

# Configure the SnsMessageBroker for AWS SNS
ADMIN_MESSAGE_BROKER=SnsMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"snsTopicArn": "arn:aws:sns:us-east-2:123456789012:xyz-hub-admin-messages"}'

# Configure the StaticWebMessageBroker for static
ADMIN_MESSAGE_BROKER=StaticWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"xyz-hub_xyz-hub_1": "8080","xyz-hub_xyz-hub_2": "8080"}'

# Configure the TargetGroupWebMessageBroker for AWS ELB Target Group
ADMIN_MESSAGE_BROKER=TargetGroupWebMessageBroker
ADMIN_MESSAGE_BROKER_CONFIG='{"targetGroupArn": "arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/xyz-hub/73e2d6bc24d8a067"}'
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE=true
ADMIN_MESSAGE_BROKER_PERIODIC_UPDATE_DELAY=30000
```

### Example json file object for S3WebMessageBroker

```json
{
    "xyz-hub_1": "8080",
    "xyz-hub_2": "8080"
}
```
