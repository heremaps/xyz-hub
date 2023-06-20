import java.net.URI

plugins {
    java
    `java-library`
    `maven-publish`
    // https://github.com/diffplug/spotless
    // gradle spotlessApply
    id("com.diffplug.spotless").version("6.11.0")
    // https://github.com/johnrengelman/shadow
    //id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "com.here.naksha"
version = rootProject.properties["version"] as String

allprojects {
    apply(plugin = "java")
    apply(plugin = "maven-publish")
    apply(plugin = "com.diffplug.spotless")
    //apply(plugin = "com.github.johnrengelman.shadow")

    val mavenUrl = rootProject.properties["mavenUrl"] as String
    val mavenUser = rootProject.properties["mavenUser"] as String
    val mavenPassword = rootProject.properties["mavenPassword"] as String

    project.group = rootProject.group

    repositories {
        maven(uri("https://repo.osgeo.org/repository/release/"))
        mavenCentral()
    }

    dependencies {
        implementation("org.jetbrains:annotations:24.0.1")

        implementation("io.vertx:vertx-core:4.4.0")
        implementation("io.vertx:vertx-web:4.4.0")
        implementation("io.vertx:vertx-web-openapi:4.4.0")
        implementation("io.vertx:vertx-config:4.4.0")
        implementation("io.vertx:vertx-web-templ-handlebars:4.4.0")
        implementation("io.vertx:vertx-web-client:4.4.0")
        implementation("io.vertx:vertx-auth-jwt:4.4.0")
        implementation("io.vertx:vertx-redis-client:4.4.0")
        implementation("io.vertx:vertx-jdbc-client:4.4.0")

        implementation("io.netty:netty-transport-native-kqueue:4.1.90.Final")
        implementation("io.netty:netty-transport-native-epoll:4.1.90.Final")

        implementation("com.fasterxml.jackson.core:jackson-core:2.14.2")
        implementation("com.fasterxml.jackson.core:jackson-annotations:2.15.1")
        implementation("com.fasterxml.jackson.core:jackson-databind:2.15.1")
        implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.2")
        implementation("com.jayway.jsonpath:json-path:2.7.0")

        implementation("com.google.flatbuffers:flatbuffers-java:23.5.9")

        implementation("com.amazonaws:aws-java-sdk-core:1.12.472")
        implementation("com.amazonaws:aws-java-sdk-s3:1.12.470")
        implementation("com.amazonaws:aws-java-sdk-lambda:1.12.472")
        implementation("com.amazonaws:aws-java-sdk-sts:1.12.471")
        implementation("com.amazonaws:aws-java-sdk-dynamodb:1.12.472")
        implementation("com.amazonaws:aws-java-sdk-sns:1.12.472")
        implementation("software.amazon.awssdk:sns:2.20.69")
        implementation("com.amazonaws:aws-java-sdk-kms:1.12.429")
        implementation("com.amazonaws:aws-java-sdk-cloudwatch:1.12.472")

        implementation("com.amazonaws:aws-lambda-java-core:1.2.2")
        implementation("com.amazonaws:aws-lambda-java-log4j2:1.5.1")

        implementation("com.google.guava:guava:31.1-jre")
        implementation("org.apache.commons:commons-lang3:3.12.0")
        implementation("net.jodah:expiringmap:0.5.10")
        implementation("org.caffinitas.ohc:ohc-core:0.7.4")
        implementation("com.vividsolutions:jts-core:1.14.0")
        implementation("com.vividsolutions:jts-io:1.14.0")
        implementation("org.geotools:gt-api:19.1")
        implementation("org.geotools:gt-referencing:19.1")
        implementation("org.geotools:gt-epsg-hsql:19.1")
        implementation("org.geotools:gt-epsg-extension:19.1")

        implementation("org.slf4j:slf4j-api:2.0.6")
        implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
        implementation("org.apache.logging.log4j:log4j-core:2.20.0")
        implementation("org.apache.logging.log4j:log4j-api:2.20.0")
        implementation("org.apache.logging.log4j:log4j-jcl:2.20.0")
        implementation("com.lmax:disruptor:3.4.4")

        implementation("org.postgresql:postgresql:42.5.4")
        implementation("commons-dbutils:commons-dbutils:1.7")
        implementation("com.mchange:mchange-commons-java:0.2.20")
        implementation("com.mchange:c3p0:0.9.5.5")
        implementation("com.zaxxer:HikariCP:5.0.1")

        testImplementation("com.jayway.restassured:rest-assured:2.9.0")
        testImplementation("org.assertj:assertj-core:3.24.2")
        testImplementation("org.awaitility:awaitility:4.2.0")
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    }

    // https://github.com/diffplug/spotless/tree/main/plugin-gradle#google-java-format
    spotless {
        java {
            encoding("UTF-8")
            // TODO: licenseHeader()
            // Allow "spotless:off" / "spotless:on" comments to toggle spotless auto-format.
            toggleOffOn()
            palantirJavaFormat()
            removeUnusedImports()
            importOrder()
            formatAnnotations()
            indentWithSpaces(3)
        }
    }

    tasks {
        test {
            useJUnitPlatform()
        }

//        shadowJar {
//            isZip64 = true
//        }

        build {
            finalizedBy(spotlessApply)
        }

// TODO: Add to service
//        shadowJar {
//            archiveClassifier.set("all")
//            mergeServiceFiles()
//            isZip64 = true
//            manifest {
//                attributes["Implementation-Title"] = "Wikvaya-Service-Core"
//                attributes["Main-Class"] = "com.here.wikvaya.core.WikvayaService"
//            }
//        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    publishing {
        repositories {
            maven {
                url = URI(mavenUrl)
                credentials.username = mavenUser
                credentials.password = mavenPassword
            }
        }

        publications {
            create<MavenPublication>("maven") {
                groupId = project.group.toString()
                artifactId = project.name
                version = project.version.toString()
                from(components["java"])
            }

            artifacts {
                file("build/libs/${project.name}-${project.version}.jar")
                file("build/libs/${project.name}-${project.version}-javadoc.jar")
                file("build/libs/${project.name}-${project.version}-sources.jar")
            }
        }
    }
}