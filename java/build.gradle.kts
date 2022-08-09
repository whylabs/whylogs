plugins {
    `java-library`
    idea
    id("com.diffplug.gradle.spotless") version ("3.28.1") apply false
    id("com.google.protobuf") version ("0.8.10") apply false
}

group = "ai.whylabs"
version = "1.0.0-b0-SNAPSHOT"
//version = "0.1.7-b1-${project.properties.getOrDefault("versionType", "SNAPSHOT")}"
extra["isReleaseVersion"] = !version.toString().endsWith("SNAPSHOT")

allprojects {
    version = version
    group = group

    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "com.google.protobuf")

    repositories {
        mavenCentral()
        maven("https://oss.sonatype.org/content/repositories/snapshots")
    }

    dependencies {
        implementation(group = "com.google.protobuf", name = "protobuf-java", version = "3.21.4")
        implementation(group = "io.grpc", name = "grpc-all", version = "1.29.0")
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    sourceSets {
        main {
            java {
                srcDirs("build/generated/source/proto/main/grpc")
                srcDirs("build/generated/source/proto/main/java")
            }
        }
    }

    protobuf {
        protoc {
            artifact = 'com.google.protobuf:protoc:3.15.5'
        }

        plugins {
            grpc {
                artifact = 'io.grpc:protoc-gen-grpc-java:1.24.0'
            }
        }

        generateProtoTasks {
            all()*.plugins {
                grpc {}
            }
        }
    }
}

subprojects {
    apply(plugin = "com.diffplug.gradle.spotless")
}
