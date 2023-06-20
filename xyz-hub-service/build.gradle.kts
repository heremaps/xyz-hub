dependencies {
    implementation(project(":here-naksha-lib-models"))
    implementation(project(":xyz-lib-psql"))
}

description = "Naksha Service"

tasks {
    test {
        enabled = false
    }
}