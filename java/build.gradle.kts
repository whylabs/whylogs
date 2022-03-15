plugins {
    `java-library`
    idea
    id("com.diffplug.gradle.spotless") version ("3.28.1") apply false
}

group = "ai.whylabs"
version = "0.2.0-b0"
//version = "0.1.7-b1-${project.properties.getOrDefault("versionType", "SNAPSHOT")}"
extra["isReleaseVersion"] = !version.toString().endsWith("SNAPSHOT")

allprojects {
    version = version
    group = group

    apply(plugin = "idea")
    apply(plugin = "java")

    repositories {
        mavenCentral()
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}

subprojects {
    apply(plugin = "com.diffplug.gradle.spotless")
}
