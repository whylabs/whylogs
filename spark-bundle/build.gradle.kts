import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
    dependencies {
        classpath("com.amazonaws:aws-java-sdk-core:1.11.766")
        classpath("com.github.jengelman.gradle.plugins:shadow:5.2.0")
    }
}

plugins {
    `java-library`
    `maven-publish`
    id("com.github.johnrengelman.shadow") version ("5.2.0")
}

val scalaVersion = project.properties.getOrDefault("scalaVersion", "2.12")
val sparkVersion = project.properties.getOrDefault("sparkVersion", "3.1.1") as String
val artifactBaseName = "whylogs-spark_${sparkVersion}-scala_$scalaVersion"
val versionString = rootProject.version

group = "com.whylabs"
version = versionString

dependencies {
    implementation(project(":core"))

    // we only depends on the output of the whylogs-spark components
    // we don't want to pull in Spark dependencies here
    implementation(project(":spark", "jar"))
    implementation("ai.whylabs:whylabs-api-client:0.1.2")
}

// Do not build the jar for this package
tasks.compileJava {
    enabled = false
}

val shadowJar: ShadowJar by tasks
shadowJar.dependsOn(":spark:build")
shadowJar.apply {
    exclude("*.properties")
    exclude("META-INF/*")
    dependencies {
        // double load logging jars is always a headache
        exclude(dependency("org.slf4j:slf4j-api"))
        // javax mismatch also causes headache
        exclude(dependency("javax.annotation:javax.annotation-api"))
    }
    // relocate core libraries
    relocate("org.apache.datasketches", "com.shaded.whylabs.org.apache.datasketches")
    relocate("com.google", "com.shaded.whylabs.com.google")
    relocate("org.checkerframework", "com.shaded.whylabs.org.checkerframework")

    // okio is consumed by songbird
    relocate("okio", "com.shaded.whylabs.okio")

    archiveFileName.set("$artifactBaseName-${versionString}.jar")
}
