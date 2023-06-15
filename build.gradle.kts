import java.net.URI

plugins {
    java
    `java-library`
    `maven-publish`
}

group = "com.here.naksha"
version = "0.1.0"

allprojects {
    apply(plugin = "java")
    apply(plugin = "maven-publish")

    val mavenUrl = rootProject.properties["mavenUrl"] as String
    val mavenUser = rootProject.properties["mavenUser"] as String
    val mavenPassword = rootProject.properties["mavenPassword"] as String

    repositories {
        maven(uri("https://repo.osgeo.org/repository/release/"))
        mavenCentral()
    }

    dependencies {
        implementation("org.jetbrains:annotations:23.0.0")

        implementation("io.vertx:vertx-core:4.4.0")
        implementation("io.vertx:vertx-web:4.4.0")
        implementation("io.vertx:vertx-web-openapi:4.4.0")
        implementation("io.vertx:vertx-config:4.4.0")
        implementation("io.vertx:vertx-web-templ-handlebars:4.4.0")
        implementation("io.vertx:vertx-web-client:4.4.0")
        implementation("io.vertx:vertx-auth-jwt:4.4.0")
        implementation("io.vertx:vertx-redis-client:4.4.0")
        implementation("io.vertx:vertx-jdbc-client:4.4.0")

        implementation("io.netty:netty-transport-native-kqueue:4.1.30.Final")
        implementation("io.netty:netty-transport-native-epoll:4.1.30.Final")

        implementation("com.fasterxml.jackson.core:jackson-core:2.14.2")
        implementation("com.fasterxml.jackson.core:jackson-annotations:2.14.2")
        implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
        implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.2")
        implementation("com.jayway.jsonpath:json-path:2.7.0")

        implementation("com.google.flatbuffers:flatbuffers-java:2.0.3")

        implementation("com.amazonaws:aws-java-sdk-core:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-s3:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-lambda:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-sts:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-dynamodb:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-sns:1.12.420")
        implementation("software.amazon.awssdk:sns:2.15.41")
        implementation("com.amazonaws:aws-java-sdk-kms:1.12.420")
        implementation("com.amazonaws:aws-java-sdk-cloudwatch:1.12.420")

        implementation("com.amazonaws:aws-lambda-java-core:1.2.1")
        implementation("com.amazonaws:aws-lambda-java-log4j2:1.5.1")

        implementation("com.google.guava:guava:31.0.1-jre")
        implementation("org.apache.commons:commons-lang3:3.12.0")
        implementation("net.jodah:expiringmap:0.5.10")
        implementation("org.caffinitas.ohc:ohc-core:0.7.0")
        implementation("com.vividsolutions:jts-core:1.14.0")
        implementation("com.vividsolutions:jts-io:1.14.0")
        implementation("org.geotools:gt-api:19.1")
        implementation("org.geotools:gt-referencing:19.1")
        implementation("org.geotools:gt-epsg-hsql:19.1")
        implementation("org.geotools:gt-epsg-extension:19.1")

        implementation("org.slf4j:slf4j-api:2.0.6")
        implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
        implementation("org.apache.logging.log4j:log4j-core:2.17.1")
        implementation("org.apache.logging.log4j:log4j-api:2.17.1")
        implementation("org.apache.logging.log4j:log4j-jcl:2.17.1")
        implementation("com.lmax:disruptor:3.4.4")

        implementation("org.postgresql:postgresql:42.4.3")
        implementation("commons-dbutils:commons-dbutils:1.7")
        implementation("com.mchange:mchange-commons-java:0.2.20")
        implementation("com.mchange:c3p0:0.9.5.5")
        implementation("com.zaxxer:HikariCP:4.0.3")

        testImplementation("com.jayway.restassured:rest-assured:2.9.0")
        testImplementation("org.assertj:assertj-core:3.21.0")
        testImplementation("org.awaitility:awaitility:4.1.1")
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    }

    tasks {
        test {
            useJUnitPlatform()
        }
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