dependencies {
    implementation(project(":xyz-lib-models"))
    implementation(project(":xyz-lib-psql"))

    implementation("com.flipkart.zjsonpatch:zjsonpatch:0.4.13")
    testImplementation("com.jayway.jsonpath:json-path:2.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

description = "XYZ Handler - Activity-Log"
