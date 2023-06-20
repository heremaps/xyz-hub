dependencies {
    implementation(project(":here-naksha-lib-core"))
    implementation(project(":here-naksha-lib-psql"))
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
    implementation("org.apache.logging.log4j:log4j-core:2.17.1")
    implementation("org.apache.logging.log4j:log4j-api:2.17.1")
    implementation("org.apache.logging.log4j:log4j-jcl:2.17.1")
    implementation("com.amazonaws:aws-lambda-java-log4j2:1.5.1")
    implementation("commons-dbutils:commons-dbutils:1.7")
    implementation("com.vividsolutions:jts-core:1.14.0")
    implementation("com.amazonaws:aws-lambda-java-core:1.2.1")
    implementation("com.amazonaws:aws-java-sdk-kms:1.12.420")
    implementation("com.mchange:mchange-commons-java:0.2.20")
    implementation("com.mchange:c3p0:0.9.5.5")
    implementation("org.postgresql:postgresql:42.4.3")
    implementation("com.zaxxer:HikariCP:4.0.3")
    implementation("com.google.crypto.tink:tink:1.5.0")
    implementation("com.google.protobuf:protobuf-java:3.16.3")
    implementation("io.vertx:vertx-core:4.4.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    testImplementation("com.jayway.jsonpath:json-path:2.7.0")
}

group = "com.here.naksha"
description = "XYZ Handler - PSQL"

tasks {
    test {
        enabled = false
    }
}