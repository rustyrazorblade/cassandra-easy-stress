/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.netflix.gradle.plugins.deb.Deb
import com.netflix.gradle.plugins.rpm.Rpm
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.redline_rpm.header.Os

plugins {
    idea
    java
    application
    alias(libs.plugins.jib)
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.nebula.ospackage)
    alias(libs.plugins.nebula.ospackage.application)
    alias(libs.plugins.ktlint)
    alias(libs.plugins.detekt)
    alias(libs.plugins.shadow)
    alias(libs.plugins.kover)
}

apply(plugin = "kotlin")

group = "org.apache.cassandra"

// Target whatever JDK is actually running the build (21 or 25 in CI) rather
// than pinning a fixed floor here -- the release-artifact-building CI jobs
// (build, build-check, create-test-artifact) are the single source of truth
// for what the shipped artifact requires, and they're pinned to JDK 21.
java {
    sourceCompatibility = JavaVersion.current()
    targetCompatibility = JavaVersion.current()
}

application {
    applicationName = "cassandra-easy-stress"
    mainClass = "org.apache.cassandra.easystress.MainKt"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.jcommander)
    implementation(libs.commons.text)
    implementation(libs.commons.math3)

    // Java driver v4
    implementation(libs.cassandra.driver.core)
    implementation(libs.jackson.module.kotlin)

    implementation(libs.reflections)

    // Logging
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.log4j.api.kotlin)
    implementation(libs.kotlin.reflect)
    // maps the datastax driver slf4j calls to log4j
    implementation(libs.log4j.slf4j18.impl)

    // needed for yaml logging configurations
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)

    // Metrics
    implementation(libs.dropwizard.metrics.core)
    implementation(libs.prometheus.simpleclient)
    implementation(libs.prometheus.simpleclient.dropwizard)
    implementation(libs.prometheus.simpleclient.httpserver)

    implementation(libs.guava)
    implementation(libs.mordant)
    implementation(libs.progressbar)
    implementation(libs.hdrhistogram)
    // Pinned to 1.22.0; the project now targets JDK 21, so the 2.x/1.23+
    // line's former JDK 17 requirement is no longer a blocker -- upgrading
    // is possible but left out of scope here.
    implementation(libs.agrona)

    // Parquet support
    implementation(libs.parquet.hadoop)
    implementation(libs.hadoop.common)
    implementation(libs.hadoop.mapreduce.client.common)

    // MCP Server dependencies
    implementation(libs.mcp.sdk) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.ktor.server.core) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.ktor.server.cio) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.ktor.server.content.negotiation) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.ktor.serialization.kotlinx.json) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation(libs.ktor.server.sse) {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    // Test dependencies
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.junit.jupiter.params)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.mockk)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.cassandra)
}

// Target whatever JDK is actually running the build, matching the java {}
// block above -- see the comment there.
tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.fromTarget(JavaVersion.current().majorVersion))
    }
}

sourceSets {
    main {
        java.srcDirs("src/main/kotlin")
    }
    test {
        java.srcDirs("src/test/kotlin")
    }
}

tasks.test {
    useJUnitPlatform()

    // Make Gradle aware of CASSANDRA_VERSION environment variable
    // This ensures tests rerun when the version changes
    val cassandraVersion = System.getenv("CASSANDRA_VERSION") ?: "5.0"
    inputs.property("cassandraVersion", cassandraVersion)

    // Pass environment variable to test JVM
    environment("CASSANDRA_VERSION", cassandraVersion)
}

// Create individual test tasks for each Cassandra version
listOf("4.0", "4.1", "5.0").forEach { version ->
    val versionName = version.replace(".", "")
    tasks.register<Test>("test$versionName") {
        group = "Verification"
        description = "Run tests against Cassandra $version"

        useJUnitPlatform()

        // Set the Cassandra version for this test task
        environment("CASSANDRA_VERSION", version)
        inputs.property("cassandraVersion", version)

        // Use same test sources and classpath as main test task
        testClassesDirs =
            sourceSets.test
                .get()
                .output.classesDirs
        classpath = sourceSets.test.get().runtimeClasspath

        doFirst {
            println()
            println("=".repeat(80))
            println("Testing Cassandra $version")
            println("=".repeat(80))
        }

        doLast {
            println()
            println("✅ Cassandra $version tests PASSED")
        }
    }
}

// Ensure sequential execution
tasks.named("test41") { mustRunAfter(tasks.named("test40")) }
tasks.named("test50") { mustRunAfter(tasks.named("test41")) }

tasks.register("testAllVersions") {
    group = "Verification"
    description = "Run tests against all Cassandra versions (4.0, 4.1, 5.0)"

    // Depend on all version-specific test tasks
    dependsOn(tasks.named("test40"), tasks.named("test41"), tasks.named("test50"))

    doFirst {
        println()
        println("=".repeat(80))
        println("Running tests against all Cassandra versions: 4.0, 4.1, 5.0")
        println("=".repeat(80))
    }

    doLast {
        println()
        println("=".repeat(80))
        println("Test Summary")
        println("=".repeat(80))
        println("✅ Cassandra 4.0: PASSED")
        println("✅ Cassandra 4.1: PASSED")
        println("✅ Cassandra 5.0: PASSED")
        println("=".repeat(80))
        println()
    }
}

tasks.register<Exec>("docs") {
    dependsOn("shadowJar")
    dependsOn("generateExamples")

    environment("CASSANDRA_EASY_STRESS_VERSION", project.version.toString())
    commandLine("docker-compose", "up", "docs")
    group = "Documentation"
    description = "Build website documentation"
}

tasks.register<Exec>("generateExamples") {
    dependsOn("shadowJar")
    commandLine("manual/generate_examples.sh")
    group = "Documentation"
    description = "Generate examples for documentation"
}

// JDK 23+ removed -XX:+ZGenerational (generational collection is ZGC's only
// mode there); keep this in sync with the JDK-version check in
// bin/cassandra-easy-stress.
val jibBaseJdkVersion = 21
val jibGcOpts =
    if (jibBaseJdkVersion >= 23) "-XX:+UseZGC" else "-XX:+UseZGC -XX:+ZGenerational"

jib {
    to {
        image = "ghcr.io/${System.getenv("GITHUB_REPOSITORY") ?: "apache/cassandra-easy-stress"}"
        tags = setOf("latest")
    }
    from {
        image = "eclipse-temurin:$jibBaseJdkVersion-jre"
    }
    container {
        // Generational ZGC by default; override at "docker run" time with
        // -e JAVA_TOOL_OPTIONS="..." to select a different collector.
        environment = mapOf("JAVA_TOOL_OPTIONS" to jibGcOpts)
    }
}

ospackage {
    os = Os.LINUX
    link("/usr/local/bin/cassandra-easy-stress", "/opt/cassandra-easy-stress/bin/cassandra-easy-stress")
    packager = "Jon Haddad"
    maintainer = "Jon Haddad"
    vendor = "Rustyrazorblade Consulting"
    url = "http://rustyrazorblade.com/cassandra-easy-stress/"
    license = "Apache License 2.0"
    description = "Stress Tool for Apache Cassandra by Rustyrazorblade Consulting"
}

tasks.named<Deb>("buildDeb") {
    distribution = "weezy,bionic,xenial,jessie"
    requires("openjdk-21-jre")
    group = "build"
}

tasks.named<Rpm>("buildRpm") {
    requires("java-21-openjdk")
    user = "root"
    group = "build"
}

tasks.register("buildAll") {
    group = "build"
    dependsOn("buildDeb")
    dependsOn("buildRpm")
    dependsOn("distTar")
}

tasks.distTar {
    compression = Compression.GZIP
    archiveExtension.set("tar.gz")
}

tasks.assemble {
    mustRunAfter(tasks.clean)
}

distributions {
    main {
        contents {
            from("LICENSE.txt") {
                into("")
            }
        }
    }
}

tasks.wrapper {
    distributionType = Wrapper.DistributionType.ALL
}

detekt {
    toolVersion = "1.23.8"
    source.setFrom(files("src/main/kotlin", "src/test/kotlin"))
    parallel = true
    config.setFrom(files("$projectDir/detekt-config.yml"))
    buildUponDefaultConfig = true
    allRules = false
    baseline = file("$projectDir/detekt-baseline.xml")
    disableDefaultRuleSets = false
    debug = false
    ignoreFailures = false
    ignoredBuildTypes = listOf("release")
    ignoredFlavors = listOf("production")
    ignoredVariants = listOf("productionRelease")
    autoCorrect = false
}

tasks.withType<Detekt>().configureEach {
    jvmTarget = "21"
}
tasks.withType<DetektCreateBaselineTask>().configureEach {
    jvmTarget = "21"
}
