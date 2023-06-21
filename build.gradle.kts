@file:Suppress("PropertyName")

import java.net.URI

plugins {
    java
    `java-library`
    `maven-publish`
    // https://github.com/diffplug/spotless
    // gradle spotlessApply
    id("com.diffplug.spotless").version("6.11.0")
    // https://github.com/johnrengelman/shadow
    id("com.github.johnrengelman.shadow") version "7.1.2"
    kotlin("jvm") version "1.8.21"
}

group = "com.here.naksha"
version = rootProject.properties["version"] as String

val jetbrains_annotations = "org.jetbrains:annotations:24.0.1"

val vertx_core = "io.vertx:vertx-core:4.4.0"
val vertx_config = "io.vertx:vertx-config:4.4.0"
val vertx_auth_jwt = "io.vertx:vertx-auth-jwt:4.4.0"
val vertx_redis_client = "io.vertx:vertx-redis-client:4.4.0"
val vertx_jdbc_client = "io.vertx:vertx-jdbc-client:4.4.0"
val vertx_web = "io.vertx:vertx-web:4.4.0"
val vertx_web_openai = "io.vertx:vertx-web-openapi:4.4.0"
val vertx_web_client = "io.vertx:vertx-web-client:4.4.0"
val vertx_web_templ = "io.vertx:vertx-web-templ-handlebars:4.4.0"

val netty_transport_native_kqueue = "io.netty:netty-transport-native-kqueue:4.1.90.Final"
val netty_transport_native_epoll = "io.netty:netty-transport-native-epoll:4.1.90.Final"

val jackson_core = "com.fasterxml.jackson.core:jackson-core:2.14.2"
val jackson_core_annotations = "com.fasterxml.jackson.core:jackson-annotations:2.15.1"
val jackson_core_databind = "com.fasterxml.jackson.core:jackson-databind:2.15.1"
val jackson_core_dataformat = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.2"

val google_flatbuffers = "com.google.flatbuffers:flatbuffers-java:23.5.9"
val google_protobuf = "com.google.protobuf:protobuf-java:3.16.3"
val google_guava = "com.google.guava:guava:31.1-jre"
val google_tink = "com.google.crypto.tink:tink:1.5.0"

val aws_core = "com.amazonaws:aws-java-sdk-core:1.12.472"
val aws_s3 = "com.amazonaws:aws-java-sdk-s3:1.12.470"
val aws_sts = "com.amazonaws:aws-java-sdk-sts:1.12.471"
val aws_dynamodb = "com.amazonaws:aws-java-sdk-dynamodb:1.12.472"
val aws_sns = "com.amazonaws:aws-java-sdk-sns:1.12.472"
val aws_kms = "com.amazonaws:aws-java-sdk-kms:1.12.429"
val aws_cloudwatch = "com.amazonaws:aws-java-sdk-cloudwatch:1.12.472"
val aws_lambda = "com.amazonaws:aws-java-sdk-lambda:1.12.472"
val aws_lambda_core = "com.amazonaws:aws-lambda-java-core:1.2.2"
val aws_lambda_log4j = "com.amazonaws:aws-lambda-java-log4j2:1.5.1"
val amazon_sns = "software.amazon.awssdk:sns:2.20.69"

val vividsolutions_jts_core = "com.vividsolutions:jts-core:1.14.0"
val vividsolutions_jts_io = "com.vividsolutions:jts-io:1.14.0"
val gt_api = "org.geotools:gt-api:19.1"
val gt_referencing = "org.geotools:gt-referencing:19.1"
val gt_epsg_hsql = "org.geotools:gt-epsg-hsql:19.1"
val gt_epsg_extension = "org.geotools:gt-epsg-extension:19.1"

val slf4j = "org.slf4j:slf4j-api:2.0.6"
val log4j_core = "org.apache.logging.log4j:log4j-core:2.20.0"
val log4j_api = "org.apache.logging.log4j:log4j-api:2.20.0"
val log4j_jcl = "org.apache.logging.log4j:log4j-jcl:2.20.0"
val log4j_slf4j = "org.apache.logging.log4j:log4j-slf4j-impl:2.20.0"

val postgres = "org.postgresql:postgresql:42.5.4"
val zaxxer_hikari = "com.zaxxer:HikariCP:5.0.1"
val commons_dbutils = "commons-dbutils:commons-dbutils:1.7"

val commons_lang3 = "org.apache.commons:commons-lang3:3.12.0"
val jodah_expiringmap = "net.jodah:expiringmap:0.5.10"
val caffinitas_ohc ="org.caffinitas.ohc:ohc-core:0.7.4"
val lmax_disruptor = "com.lmax:disruptor:3.4.4"
val mchange_commons = "com.mchange:mchange-commons-java:0.2.20"
val mchange_c3p0 = "com.mchange:c3p0:0.9.5.5"

val jayway_jsonpath = "com.jayway.jsonpath:json-path:2.7.0"
val jayway_restassured = "com.jayway.restassured:rest-assured:2.9.0"
val assertj_core = "org.assertj:assertj-core:3.24.2"
val awaitility = "org.awaitility:awaitility:4.2.0"
val junit_jupiter = "org.junit.jupiter:junit-jupiter:5.9.2"

val flipkart_zjsonpatch = "com.flipkart.zjsonpatch:zjsonpatch:0.4.13"

val mavenUrl = rootProject.properties["mavenUrl"] as String
val mavenUser = rootProject.properties["mavenUser"] as String
val mavenPassword = rootProject.properties["mavenPassword"] as String

// Apply general settings to all sub-projects
subprojects {
    // All subprojects should be in the naksha group (for artifactory) and have the same version!
    group = rootProject.group
    version = rootProject.version

    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "java-library")
    apply(plugin = "kotlin")

    repositories {
        maven(uri("https://repo.osgeo.org/repository/release/"))
        mavenCentral()
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

        build {
            finalizedBy(spotlessApply)
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }
}

// Note: We normally would want to move these settings into dedicated files in the subprojects,
//       but if we do that, the shared section at the end (about publishing and shadow-jar) are
//       not that easy AND, worse: We can't share the constants for the dependencies.

// Shared dependencies
subprojects {
    dependencies {
        implementation(jetbrains_annotations)
        implementation(jackson_core_annotations)

        testImplementation(junit_jupiter)
    }
}

project(":here-naksha-lib-core") {
    description = "Naksha Core Library"
    dependencies {
        // We include this and expose it via currentLogger() to others!
        api(slf4j)
        implementation(google_guava)
        implementation(commons_lang3)
        implementation(vividsolutions_jts_core)
        api(jackson_core)
        api(jackson_core_databind)
        api(jackson_core_dataformat)
        api(jackson_core_annotations)
        implementation(google_flatbuffers)
    }
}

project(":here-naksha-lib-psql") {
    description = "Naksha PostgresQL Storage Library"
    dependencies {
        implementation(project(":here-naksha-lib-core"))

        implementation(commons_lang3)
        implementation(postgres)
        implementation(zaxxer_hikari)
        implementation(commons_dbutils)
        implementation(vividsolutions_jts_core)
    }
}

project(":here-naksha-handler-activitylog") {
    description = "Naksha Activity Log Handler"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))

        implementation(flipkart_zjsonpatch)
        testImplementation(jayway_jsonpath)
    }
}

project(":here-naksha-lib-httpserver") {
    description = "Naksha HTTP Server Library"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
    }
}

project(":here-naksha-handler-http") {
    description = "Naksha Http Handler"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        testImplementation(project(":here-naksha-lib-httpserver"))

        implementation(vividsolutions_jts_core)

        testImplementation(jayway_jsonpath)
    }
}

project(":here-naksha-handler-psql") {
    description = "Naksha PostgresQL Handler"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))

        implementation(commons_lang3)
        implementation(commons_dbutils)
        implementation(vividsolutions_jts_core)
        implementation(aws_kms)
        implementation(mchange_commons)
        implementation(mchange_c3p0)
        implementation(postgres)
        implementation(zaxxer_hikari)
        implementation(google_tink)
        implementation(google_protobuf)
        implementation(vertx_core)

        testImplementation(jayway_jsonpath)
    }

    tasks {
        test {
            enabled = false
        }
    }
}

// Ensure that libraries published to artifactory, while the application generates a shadow-jar.
subprojects {
    if (project.name != "here-naksha-hub") {
        // This is library, publish to maven artifactory
        apply(plugin = "maven-publish")
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
    } else {
        // This is the Naksha-Hub, an application, only create the shadow-jar.
        apply(plugin = "com.github.johnrengelman.shadow")

        tasks {
            shadowJar {
                archiveClassifier.set("all")
                mergeServiceFiles()
                isZip64 = true
                manifest {
                    attributes["Implementation-Title"] = "Naksha Service"
                    attributes["Main-Class"] = "com.here.naksha.hub.Main"
                }
            }
        }
    }
}