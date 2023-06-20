dependencies {
    implementation(project(":here-naksha-lib-core"))
    implementation("org.postgresql:postgresql:42.4.3")
    implementation("com.zaxxer:HikariCP:4.0.3")
    implementation("commons-dbutils:commons-dbutils:1.7")
    implementation("com.vividsolutions:jts-core:1.14.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

group = "com.here.naksha"
description = "XYZ Lib - PSQL"

tasks {
    test {
        enabled = false
    }
}
