@file:Suppress("PropertyName")

import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

repositories {
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

plugins {
    java
    `java-library`
    `java-test-fixtures`
    `maven-publish`
    // https://github.com/diffplug/spotless
    // gradle spotlessApply
    id("com.diffplug.spotless").version("6.22.0")
    // https://github.com/johnrengelman/shadow
    id("com.github.johnrengelman.shadow") version "8.1.1"
    // Don't apply for all projects, we individually only apply where Kotlin is used.
    kotlin("jvm") version "1.8.21" apply false
    // overall code coverage
    //jacoco
    id("jacoco-report-aggregation")
    id("org.sonarqube") version "4.4.1.3373"
}

group = "com.here.naksha"
version = rootProject.properties["version"] as String

val jetbrains_annotations = "org.jetbrains:annotations:24.0.1"

val vertx_version = "4.5.0"
val vertx_core = "io.vertx:vertx-core:$vertx_version"
val vertx_config = "io.vertx:vertx-config:$vertx_version"
val vertx_auth_jwt = "io.vertx:vertx-auth-jwt:$vertx_version"
val vertx_redis_client = "io.vertx:vertx-redis-client:$vertx_version"
val vertx_jdbc_client = "io.vertx:vertx-jdbc-client:$vertx_version"
val vertx_web = "io.vertx:vertx-web:$vertx_version"
val vertx_web_openapi = "io.vertx:vertx-web-openapi:$vertx_version"
val vertx_web_client = "io.vertx:vertx-web-client:$vertx_version"
val vertx_web_templ = "io.vertx:vertx-web-templ-handlebars:$vertx_version"

val netty_transport_native_kqueue = "io.netty:netty-transport-native-kqueue:4.1.90.Final"
val netty_transport_native_epoll = "io.netty:netty-transport-native-epoll:4.1.90.Final"

val jackson_core = "com.fasterxml.jackson.core:jackson-core:2.15.2"
val jackson_core_annotations = "com.fasterxml.jackson.core:jackson-annotations:2.15.2"
val jackson_core_databind = "com.fasterxml.jackson.core:jackson-databind:2.15.2"
val jackson_core_dataformat = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.2"

var snakeyaml = "org.yaml:snakeyaml:1.33";

val google_flatbuffers = "com.google.flatbuffers:flatbuffers-java:23.5.9"
val google_protobuf = "com.google.protobuf:protobuf-java:3.16.3"
val google_guava = "com.google.guava:guava:31.1-jre"
val google_tink = "com.google.crypto.tink:tink:1.5.0"

val aws_bom = "software.amazon.awssdk:bom:2.25.19"
val aws_s3="software.amazon.awssdk:s3"

val jts_core = "org.locationtech.jts:jts-core:1.19.0"
val jts_io = "org.locationtech.jts:jts-io:1.19.0"
val gt_api = "org.geotools:gt-api:19.1"
val gt_referencing = "org.geotools:gt-referencing:19.1"
val gt_epsg_hsql = "org.geotools:gt-epsg-hsql:19.1"
val gt_epsg_extension = "org.geotools:gt-epsg-extension:19.1"

val spatial4j = "com.spatial4j:spatial4j:0.5"

val slf4j_api = "org.slf4j:slf4j-api:2.0.6"
val slf4j_console = "org.slf4j:slf4j-simple:2.0.6";
val jcl_slf4j = "org.slf4j:jcl-over-slf4j:2.0.12"


val log4j_core = "org.apache.logging.log4j:log4j-core:2.20.0"
val log4j_api = "org.apache.logging.log4j:log4j-api:2.20.0"
val log4j_jcl = "org.apache.logging.log4j:log4j-jcl:2.20.0"
val log4j_slf4j = "org.apache.logging.log4j:log4j-slf4j2-impl:2.20.0"

val postgres = "org.postgresql:postgresql:42.5.4"
//val zaxxer_hikari = "com.zaxxer:HikariCP:5.1.0"
val commons_dbutils = "commons-dbutils:commons-dbutils:1.7"

val commons_lang3 = "org.apache.commons:commons-lang3:3.12.0"
val jodah_expiringmap = "net.jodah:expiringmap:0.5.10"
val caffinitas_ohc = "org.caffinitas.ohc:ohc-core:0.7.4"
val lmax_disruptor = "com.lmax:disruptor:3.4.4"
val mchange_commons = "com.mchange:mchange-commons-java:0.2.20"
val mchange_c3p0 = "com.mchange:c3p0:0.9.5.5"

val jayway_jsonpath = "com.jayway.jsonpath:json-path:2.7.0"
val jayway_restassured = "com.jayway.restassured:rest-assured:2.9.0"
val assertj_core = "org.assertj:assertj-core:3.24.2"
val awaitility = "org.awaitility:awaitility:4.2.0"
val junit_jupiter = "org.junit.jupiter:junit-jupiter:5.9.2"
val junit_params = "org.junit.jupiter:junit-jupiter-params:5.9.2"
val mockito = "org.mockito:mockito-core:5.8.0"
val test_containers = "org.testcontainers:testcontainers:1.19.3"
val wiremock =  "org.wiremock:wiremock:3.3.1"

val flipkart_zjsonpatch = "com.flipkart.zjsonpatch:zjsonpatch:0.4.13"
val json_assert = "org.skyscreamer:jsonassert:1.5.1"
val resillience4j_retry = "io.github.resilience4j:resilience4j-retry:2.0.0"

val otel = "io.opentelemetry:opentelemetry-api:1.28.0"

val cytodynamics = "com.linkedin.cytodynamics:cytodynamics-nucleus:0.2.0"

val projectRepoURI = getRequiredPropertyFromRootProject("projectRepoURI")
val mavenUrl = getRequiredPropertyFromRootProject("mavenUrl")
val mavenUser = getRequiredPropertyFromRootProject("mavenUser")
val mavenPassword = getRequiredPropertyFromRootProject("mavenPassword")

/*
    Overall coverage of subproject - it might be different for different subprojects
    Configurable per project - see `setOverallCoverage`
 */
val minOverallCoverageKey: String = "minOverallCoverage"
val defaultOverallMinCoverage: Double = 0.8 // Don't decrease me!

/*

    IMPORTANT: api vs implementation

    We need to differ between libraries (starting with "here-naksha-lib") and other parts of
    the project. For the Naksha libraries we need to select "api" for any dependency, that is
    needed for the public API (should be usable by the user of the library), while
    “implementation” should be used for all test dependencies, or dependencies that must not be
    used by the final users.

 */

// Apply general settings to all sub-projects
subprojects {
    // All subprojects should be in the naksha group (for artifactory) and have the same version!
    group = rootProject.group
    version = rootProject.version

    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "java-library")
    apply(plugin = "java-test-fixtures")
    apply(plugin = "jacoco")

    repositories {
        maven(uri("https://repo.osgeo.org/repository/release/"))
        mavenCentral()
    }

    // https://github.com/diffplug/spotless/tree/main/plugin-gradle
    spotless {
        java {
            // excluding tests where Builder pattern gets broken by palantir
            targetExclude("src/test/**")
            encoding("UTF-8")
            val YEAR = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy"))
            licenseHeader("""
/*
 * Copyright (C) 2017-$YEAR HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
""")
            // Allow "spotless:off" / "spotless:on" comments to toggle spotless auto-format.
            toggleOffOn()
            removeUnusedImports()
            importOrder()
            formatAnnotations()
            // https://github.com/diffplug/spotless/issues/1774
            palantirJavaFormat("2.39.0")
            indentWithTabs(4)
            indentWithSpaces(2)
        }
    }

    tasks {
        test {
            maxHeapSize = "4g"
            useJUnitPlatform()
            testLogging {
                showStandardStreams = true
                exceptionFormat = TestExceptionFormat.FULL
                events("standardOut", "started", "passed", "skipped", "failed")
            }
            afterTest(KotlinClosure2(
                    { descriptor: TestDescriptor, result: TestResult ->
                        val totalTime = result.endTime - result.startTime
                        println("Total time of $descriptor.name was $totalTime")
                    }
            ))
        }

        compileJava {
            finalizedBy(spotlessApply)
        }

        // Suppress Javadoc errors (we document our checked exceptions).
        javadoc {
            options {
                this as StandardJavadocDocletOptions
                addBooleanOption("Xdoclint:none", true)
                addStringOption("Xmaxwarns", "1")
            }
        }

        jacocoTestReport {
            dependsOn(test)
            reports {
                xml.required = true
            }
        }

        jacocoTestCoverageVerification {
            dependsOn(jacocoTestReport)
            violationRules {
                rule {
                    limit {
                        minimum = getOverallCoverage().toBigDecimal()
                    }
                }
            }
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    // Fix transitive dependencies.

    dependencies {
        implementation(snakeyaml) {
            // https://stackoverflow.com/questions/70154082/getting-java-lang-nosuchmethoderror-org-yaml-snakeyaml-yaml-init-while-runnin
            version {
                strictly("1.33")
            }
        }
        implementation(platform(aws_bom))
    }

    // Shared dependencies.

    if (name.startsWith("here-naksha-lib")) {
        // TODO: We need to expose JTS, but actually we need to upgrade it first.
        dependencies {
            api(jetbrains_annotations)
            api(slf4j_api)
            api(jackson_core)
            api(jackson_core_databind)
            api(jackson_core_dataformat)
            api(jackson_core_annotations)
        }
    } else {
        dependencies {
            implementation(jetbrains_annotations)
            implementation(slf4j_api)
            implementation(jackson_core)
            implementation(jackson_core_databind)
            implementation(jackson_core_dataformat)
            implementation(jackson_core_annotations)
        }
    }
    dependencies {
        testImplementation(log4j_slf4j)
        testImplementation(log4j_api)
        testImplementation(log4j_core)
        testImplementation(junit_jupiter)
        testImplementation(junit_params)
        testFixturesApi(junit_jupiter)
    }
}

// Note: We normally would want to move these settings into dedicated files in the subprojects,
//       but if we do that, the shared section at the end (about publishing and shadow-jar) are
//       not that easy AND, worse: We can't share the constants for the dependencies.

project(":here-naksha-lib-core") {
    description = "Naksha Core Library"
    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        withJavadocJar()
        withSourcesJar()
    }
    dependencies {
        // Can we get rid of this?
        implementation(google_guava)
        implementation(commons_lang3)
        implementation(jts_core)
        implementation(google_flatbuffers)
        testImplementation(mockito)
        testImplementation(json_assert)
    }
    setOverallCoverage(0.0) // only increasing allowed!
}

project(":here-naksha-lib-heapcache") {
    description = "Naksha Heap Caching Library"
    java {
        withJavadocJar()
        withSourcesJar()
    }
    dependencies {
        api(project(":here-naksha-lib-core"))
        testImplementation(mockito)
        implementation(jts_core)
    }
    setOverallCoverage(0.5) // only increasing allowed!
}

project(":here-naksha-lib-psql") {
    description = "Naksha PostgresQL Storage Library"
    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        withJavadocJar()
        withSourcesJar()
    }
    dependencies {
        api(project(":here-naksha-lib-core"))

        implementation(commons_lang3)
        implementation(postgres)
        //implementation(zaxxer_hikari)
        implementation(commons_dbutils)
        implementation(jts_core)

        testImplementation(mockito)
        testImplementation(spatial4j)
    }
    setOverallCoverage(0.0) // only increasing allowed!
}

project(":here-naksha-storage-http") {
    description = "Naksha Http Storage Module"
    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        withJavadocJar()
        withSourcesJar()

    }
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-common-http"))

        implementation(commons_lang3)

        testImplementation(mockito)
    }
    setOverallCoverage(0.0) // only increasing allowed!

}


project(":here-naksha-lib-view") {
    description = "Naksha View Library"
    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        withJavadocJar()
        withSourcesJar()
    }
    dependencies {
        api(project(":here-naksha-lib-core"))

        implementation(commons_lang3)
        testImplementation(mockito)
        testImplementation(project(":here-naksha-lib-psql"))
        testImplementation(jts_core)
    }
    setOverallCoverage(0.0) // only increasing allowed!
}

/*
project(":here-naksha-lib-extension") {
    description = "Naksha Extension Library"
    dependencies {
        api(project(":here-naksha-lib-core"))
    }
    setOverallCoverage(0.4) // only increasing allowed!
}
*/

project(":here-naksha-handler-activitylog") {
    description = "Naksha Activity Log Handler"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))
        implementation(project(":here-naksha-lib-handlers"))

        implementation(flipkart_zjsonpatch)
        testImplementation(jayway_jsonpath)
        testImplementation(mockito)
        testImplementation(json_assert)
        testImplementation(testFixtures(project(":here-naksha-lib-core")))
    }
    setOverallCoverage(0.4) // only increasing allowed!
}

/*
project(":here-naksha-handler-http") {
    description = "Naksha Http Handler"
    apply(plugin = "kotlin")
    tasks {
        // Note: Using compileKotlin {} does not work due to a bug in the Kotlin DSL!
        //       It only works, when applying the Kotlin plugin for all projects.
        withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
            kotlinOptions.jvmTarget = JavaVersion.VERSION_17.toString()
            finalizedBy(spotlessApply)
        }
    }
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        testImplementation(project(":here-naksha-lib-extension"))

        implementation(jts_core)

        testImplementation(jayway_jsonpath)
    }
}
*/

configurations.implementation {
    exclude(module = "commons-logging")
}

project(":here-naksha-lib-handlers") {
    description = "Naksha Handlers library"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))
        implementation(project(":here-naksha-lib-view"))
        implementation(project(":here-naksha-storage-http"))

        implementation(commons_lang3)
        implementation(commons_dbutils)

        testImplementation(mockito)
        testImplementation(json_assert)
        testImplementation(testFixtures(project(":here-naksha-lib-core")))

        setOverallCoverage(0.0)
    }
}

project(":here-naksha-lib-ext-manager") {
    description = "Naksha Extension Manager Library"
    dependencies {
        api(project(":here-naksha-lib-core"))

        implementation(aws_s3)
        implementation(jcl_slf4j)
        implementation(cytodynamics)
        testImplementation(mockito)
    }
    setOverallCoverage(0.0) // only increasing allowed!
}

//try {
project(":here-naksha-lib-hub") {
    description = "NakshaHub library"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))
        implementation(project(":here-naksha-lib-handlers"))
        implementation(project(":here-naksha-lib-ext-manager"))

        implementation(commons_lang3)
        implementation(jts_core)
        implementation(postgres)
        implementation(aws_s3)

        testImplementation(json_assert)
        testImplementation(mockito)
    }
    setOverallCoverage(0.2) // only increasing allowed!
}
//} catch (ignore: UnknownProjectException) {
//}


//try {
project(":here-naksha-app-service") {
    description = "Naksha Service"
    dependencies {
        implementation(project(":here-naksha-lib-core"))
        implementation(project(":here-naksha-lib-psql"))
        implementation(project(":here-naksha-storage-http"))
        //implementation(project(":here-naksha-lib-extension"))
        implementation(project(":here-naksha-lib-hub"))
        implementation(project(":here-naksha-common-http"))

        implementation(log4j_slf4j)
        implementation(log4j_api)
        implementation(log4j_core)
        implementation(otel)
        implementation(commons_lang3)
        implementation(jts_core)
        implementation(postgres)
        implementation(vertx_core)
        implementation(vertx_auth_jwt)
        implementation(vertx_web)
        implementation(vertx_web_client)
        implementation(vertx_web_openapi)
        implementation(project(":here-naksha-handler-activitylog"))

        testImplementation(json_assert)
        testImplementation(resillience4j_retry)
        testImplementation(test_containers)
        testImplementation(testFixtures(project(":here-naksha-lib-core")))
        testImplementation(wiremock)
    }
    setOverallCoverage(0.25) // only increasing allowed!
}
//} catch (ignore: UnknownProjectException) {
//}

subprojects {
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
                pom {
                    url = "https://${projectRepoURI}"
                    licenses {
                        license {
                            name = "The Apache License, Version 2.0"
                            url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                        }
                    }
                    scm {
                        connection = "scm:git:https://${projectRepoURI}.git"
                        developerConnection = "scm:git:ssh://git@${projectRepoURI}.git"
                        url = "https://${projectRepoURI}"
                    }
                }
            }

            artifacts {
                file("build/libs/${project.name}-${project.version}.jar")
                file("build/libs/${project.name}-${project.version}-javadoc.jar")
                file("build/libs/${project.name}-${project.version}-sources.jar")
            }
        }
    }
}

// For publishing root project (including shaded jar)
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
            pom {
                url = "https://${projectRepoURI}"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                scm {
                    connection = "scm:git:https://${projectRepoURI}.git"
                    developerConnection = "scm:git:ssh://git@${projectRepoURI}.git"
                    url = "https://${projectRepoURI}"
                }
            }
        }

        artifacts {
            file("build/libs/${project.name}-${project.version}.jar")
            file("build/libs/${project.name}-${project.version}-all.jar")
        }
    }
}

// Create the fat jar for the whole Naksha.
rootProject.dependencies {
    //This is needed, otherwise the blank root project will include nothing in the fat jar
    implementation(project(":here-naksha-app-service"))
}
// to include license files in Jar
sourceSets {
    main {
        resources {
            setSrcDirs(listOf(".")).setIncludes(listOf("LICENSE","HERE_NOTICE"))
        }
    }
}

rootProject.tasks.shadowJar {
    //Have all tests run before building the fat jar
    dependsOn(allprojects.flatMap { it.tasks.withType(Test::class) })
    archiveClassifier.set("all")
    mergeServiceFiles()
    isZip64 = true
    manifest {
        attributes["Implementation-Title"] = "Naksha Service"
        attributes["Main-Class"] = "com.here.naksha.app.service.NakshaApp"
    }
}

// print app version
rootProject.tasks.register("printAppVersion") {
    println(rootProject.version)
}

fun Project.setOverallCoverage(minOverallCoverage: Double) {
    ext.set(minOverallCoverageKey, minOverallCoverage)
}

fun Project.getOverallCoverage(): Double {
    return if (ext.has(minOverallCoverageKey)) {
        ext.get(minOverallCoverageKey) as? Double
                ?: throw IllegalStateException("Property '$minOverallCoverageKey' is expected to be Double")
    } else {
        defaultOverallMinCoverage
    }
}

fun getRequiredPropertyFromRootProject(propertyKey: String): String {
    return rootProject.properties[propertyKey] as? String ?: throw IllegalArgumentException("""
        Not found required property: $propertyKey. 
        Check your 'gradle.properties' file (in both project and ~/.gradle directory)
        """.trimIndent()
    )
}
