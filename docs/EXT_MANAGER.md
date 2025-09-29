# Extension Manager

Extension Manager is a Naksha component within Naksha framework to load Naksha compliant Extensions dynamically and execute it as and when required with the help of `IEventHandler`. It decouples the deployment of extensions from Naksha service. Extensions can be deployed independently and it doesn't require Naksha service re-start. Once deployed these extensions are then loaded in separate class loader and used within Naksha application via Extension manager. This also helps to overcome runtime exceptions related to dependency conflicts. It uses an open source library `com.linkedin.cytodynamics:cytodynamics-nucleus:0.2.0` for classloader. 

## Configuration

Extension manager configuration is defined by `extensionConfigParams` attribute as part of Naksha startup config [NakshaHubConfig](../here-naksha-lib-hub/src/main/java/com/here/naksha/lib/hub/NakshaHubConfig.java).<br>
The overall config structure is the same regardless of where extensions are stored (S3 bucket or local machine). The only difference is the value of `extensionsRootPath`.

### Sample Configuration

```json
{
    "id": "extension-config",
    "type": "Config",
    // ... other config parameters
    "extensionConfigParams": {
        "whitelistClasses": [
            "java.*",
            "javax.*",
            "com.here.*",
            "jdk.internal.reflect.*",
            "com.sun.*",
            "org.w3c.dom.*",
            "sun.misc.*"
        ],
        "intervalms": 30000,
        "extensionsRootPath": "s3://naksha-pvt-releases/extensions/"
         // or for local machine "file:///Users/User/Desktop/extensions/"
    }
}
```

**Notes:**
- On Windows, the equivalent local extensionsRootPath would be: `file:///C:/Users/User/Desktop/extensions/`
- The local path must always begin with `file://`, regardless of the operating system.


It contains below fields:- 

| Property           | Type             | Meaning                                                                                                                                                                                                                                                                                                      |
|--------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| whitelistClasses   | String[]         | It contains the list of classes which have common implementation and can be exposed by parent classloader (Naksha service) to child classloader (Extension Jars) during runtime. Default values are "java.*", "javax.*", "com.here.*", "jdk.internal.reflect.*", "com.sun.*", "org.w3c.dom.*", "sun.misc.*". |
| intervalms         | Long             | Value in Milliseconds, It is being used to create Extension config expiry when the previously loaded config gets expired and so after that, system should make another attempt to refresh the extension config. i.e expiry = System.currentMillis + intervalms                                               |
| extensionsRootPath | String           | It represents the extension root path where all extension jars are placed. It can be either local or AWS S3 folder path.                                                                                                                                                                                     |


## Folder Structure

Extension manager expects all extension should have a folder under configured extension root path. All Extension should have an ID and it's folder should be created with the same ID. Every extension folder should contain below files:-

- jar - Extension fat jar
- latest-{ENV}.txt - Text file which contains jar version. This value is being used in determining extension configuration json file. Here {ENV} is the environment name the Naksha service is running against.
- {ExtensionID}-{VERSION}.{ENV}.json - It is a json file, which contains extension specific configuration. {ExtensionID} is the ID of Extension. {VERSION} is resolved value determined from latest-{ENV}.txt. And {ENV} is the environment name. This file should have below attributes 
  - id - mandatory - String representing Extension ID
  - type - mandatory - XYZFeature type. Default value is "Extension".
  - url - mandatory - Complete path of jar file (S3 URI or local file path)
  - version - mandatory - Extension version being deployed.
  - initClassName - optional - Full class name. If configured the Extension manager will load this class and instantiate it.   
  - properties - optional - Extension specific custom/private properties which is required specifically for that extension. For example, remote URL or Database details. 

### Sample Folder Structure

Below example illustrates the folder structure for an extension `foo` deployed in the `dev` environment.

```text
Root path: <extensionsRootPath> (S3 or local)

<extensionsRootPath>
|___foo
    |___latest-dev.txt  
    |___foo-1.0.0.dev.json
    |___foo-1.0.0.jar
```

Content of `latest-dev.txt`:
```text
1.0.0
```

Content of `foo-1.0.0.dev.json`:
```json
{
  "id" : "foo",
  "type": "Extension",
  "url":"<extensionsRootPath>/foo/foo-1.0.0.jar",
  "version": "1.0.0",
  "initClassName":"",
  "properties": {
    "whitelistClasses": ["java.*", "javax.*", "com.here.*", "jdk.internal.reflect.*", "com.sun.*", "org.w3c.dom.*", "sun.misc.*", "org.locationtech.jts.*", "org.xml.sax.*", "org.slf4j.*"]
    // custom/private properties relevant for that extension
  }
}
```

**Notes:**
- For S3: `<extensionsRootPath>` would be something like `s3://naksha-pvt-releases/extensions`.
- For local: `<extensionsRootPath>` would be something like `file:///Users/User/Desktop/extensions`(Windows example: `file:///C:/Users/User/Desktop/extensions`).
- The whitelistClasses defined at the Extension level take precedence over those defined in the Naksha startup configuration.

## Extension Loading

Loading of Extension into Naksha service is managed by 2 components:

### Naksha Hub - `lib-hub`

- Naksha Hub performs the job of scanning extension directories and build extension configuration. 
  - It iterates over each extension directories and looks for respective environment's latest-{ENV}.txt file. 
  - It reads it's content and decode the extension configuration file name. Which is in the form of {ExtensionID}-{VERSION}.{ENV}.json
  - Once configuration file name is decoded it reads this file content and builds extension configuration.

### Extension Manager - `lib-ext-manager`

- Extension Manager calls Naksha Hub to get Extension configuration after every `x` interval as defined in `itervalms`. In return Naksha Hub provides the list of extension configuration.
  - Extension Manager iterates over each configuration and download respective extension jar on local machine.
  - It loads the jar using Cytodynamic class loader
  - It caches the class loader as well as the extension configuration in a Map against `ExtensionId` as key.
  - It also removes mapping of any existing extension from cache for which no configuration found in latest request.
  - Once all extensions are loaded, It sleeps till configuration expiry time is reached. After that it again goes back to Naksha Hub to request for fresh configuration and this cycle goes on.

## Integration With Naksha PluginCache

Naksha app caches constructors of classes in [PluginCache](../here-naksha-lib-core/src/main/java/com/here/naksha/lib/core/models/PluginCache.java). Plugin-cache also caches and maintains Extension's constructors separately. Whenever a constructor of IEventHandler with extensionId requested:
- It tries to resolve the call by finding respective constructor from local cache.
- If it didn't find any then It calls Extension Manager to get the extension's class loader (using extensionId) 
- Once ClassLoader is retrieved, it loads the respective class and looks for available compatible constructors.
- Then it caches the constructor against the extensionId, and it's respective class.
- The same constructor then returned to the caller.

