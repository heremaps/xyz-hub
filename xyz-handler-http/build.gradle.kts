dependencies {
    implementation(project(":here-naksha-lib-core"))
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
    implementation("org.apache.logging.log4j:log4j-core:2.17.1")
    implementation("org.apache.logging.log4j:log4j-api:2.17.1")
    implementation("org.apache.logging.log4j:log4j-jcl:2.17.1")
    implementation("com.vividsolutions:jts-core:1.14.0")
    testImplementation(project(":here-naksha-lib-httpserver"))
    testImplementation("com.jayway.jsonpath:json-path:2.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

description = "XYZ Handler - HTTP(s)"
