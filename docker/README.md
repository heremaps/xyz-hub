### Running Naksha in container

Naksha can be run in container as well. So far, only locally build image can be used.\
To get Naksha container running, one must do the following:

1) CD into the root project directory (we need this because build context needs to access files that
   don't belong to `docker` directory).
     ```shell
    cd ..
    ```
2) Build the fat jar:
     ```shell
    ./gradlew shadowJar
    ```
3) Build the local image:\
   If your host's architecture is arm64 (ie you're using MacOS with Apple Silicon chip):
    ```shell
   docker build -t local-naksha-app -f docker/Dockerfile .
    ```
   For other architectures, you can specify `ARCHITECTURE` build argument that corresponds to different base image repository. For example, when running on amd64 chips (ie MacOS with Intel processors):
   ```shell
   docker build -t local-naksha-app -f docker/Dockerfile --build-arg ARCHITECTURE=amd64 .
   ```
   Other possible architectures that are supported can be found [on these Docker official images docs](https://github.com/docker-library/official-images#architectures-other-than-amd64). For more details, refer to [docs of our base image (Eclipse Temurin)](https://hub.docker.com/_/eclipse-temurin).
   
4) Run the container for the first time:\
   There are two optional environment variables that one can specify when running Naksha conrtainer
    - `NAKSHA_CONFIG_ID`: id of naksha configaration to use, `test-config` by default
    - `NAKSHA_ADMIN_DB_URL`: url of database for Naksha app to
      use, `jdbc:postgresql://host.docker.internal:5432/postgres?user=postgres&password=password&schema=naksha&app=naksha_local&id=naksha_admin_db`
      by default

   When connecting Naksha app to database, one has to consider container networking - if your
   database is running locally, then when specifying its host you should use `host.docker.internal` (see default URL above) instead of `localhost`/`127.0.0.1` (docker's default network mode is isolated `bridge` so the `localhost` for container and host are 2 different things) .\
   Putting it all together the typical command you would use is:
   ```shell
   docker run \
      --name=naksha-app \
      -p 8080:8080 \
      local-naksha-app
    ```
   or with custom config / admin db URL:
   ```shell
   docker run \
      --name=naksha-app \
      --env NAKSHA_CONFIG_ID=<your Naksha config id> \
      --env NAKSHA_ADMIN_DB_URL=<your DB uri that Naksha should use> \
      -p 8080:8080 \
      local-naksha-app
    ```
   
5) Verify your instance is up and running by accessing local Swagger: http://localhost:8080/hub/swagger/index.html

### Additional remarks

#### Running in detached mode 

Starting the container as in the sample above will hijack your terminal. To avoid this pass `-d`
flag (as in "detached")

   ```shell
   > docker run --name=naksha-app -p 8080:8080 -d local-naksha-app
   ```

#### Tailing logs

If you want to tail logs of your running container (ie when you detached it before), you can
use [docker logs](https://docs.docker.com/reference/cli/docker/container/logs/) as in the sample:

   ```shell
   docker logs -f --tail 10 naksha-app   
   ```

The command above will start tailing logs from container with the name `naksha-app` and also print last 10
lines.

#### Stopping / killing container

To stop the running container simply run:

   ```shell
   docker stop naksha-app 
   ```

Stopping is graceful, meaning - it sends `SIGTERM` to the process so the app will have some time to
perform the cleanup.\
If you need to stop the container immediately, use `docker kill naksha-app` - the main difference
is that instead of `SIGTERM` the process will receive `SIGKILL`.

#### Running stopped / killed container

If your run configuration (ports, environments etc - basically all args that you passed to the `run` command) hasn't changed and you just want to respawn Naksha, use `start`:
```shell
docker start naksha-app
```

This will bring your container back to life in detached mode. To get live logs again, refer to [tailing logs](#tailing-logs).

#### Failure when running container multiple times

If you stop/kill your container and then `run`(nod `start`!) it again, it might happen that you'll see the following error:
```shell
Error: creating container storage: the container name "naksha-app" is already in use by <id>.
You have to remove that container to be able to reuse that name: that name is already in use
```

That means that your container engine (like docker / podman) has some uncleared data associated with given container name. The quickest fix is to remove it and then run again.
```shell
docker rm naksha-app
```

#### Removing the image

If you ever need to clean the image, use one of the below (the latter is simply the alias of the
former).

   ```
   > docker image rm local-naksha-app
   > docker rmi local-naksha-app
   ```

