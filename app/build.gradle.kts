plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.8.0"
    id("com.ncorti.ktfmt.gradle") version "0.13.0"

    application
}

repositories {
    mavenLocal {
        content {
            includeVersionByRegex(".*", ".*", ".*-SNAPSHOT")
        }
    }
    mavenCentral()
}

//val PULSAR_VERSION = "3.3.7"
val PULSAR_VERSION = "4.1.0-SNAPSHOT"
//val PULSAR_VERSION = "4.0.6"
//val PULSAR_VERSION = "3.1.0-SNAPSHOT"

dependencies {
    implementation("ch.qos.logback:logback-core:1.4.11")
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("org.apache.avro:avro:1.11.2")
    implementation("org.testcontainers:pulsar:1.19.0")

    implementation("org.apache.pulsar:pulsar-client:$PULSAR_VERSION")
    implementation("org.apache.pulsar:pulsar-client-admin:$PULSAR_VERSION")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("dlq.issue.AppKt")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named("compileKotlin") {
    dependsOn("ktfmtFormat")
}
