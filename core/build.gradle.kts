import com.google.protobuf.gradle.proto
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc


buildscript {
    dependencies {
        classpath("com.amazonaws:aws-java-sdk-core:1.11.766")
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.12")
    }
}

plugins {
    `java-library`
}

apply(plugin = "com.google.protobuf")

group = rootProject.group
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

dependencies {
    api("org.slf4j:slf4j-api:1.7.27")
    api("org.apache.datasketches:datasketches-java:1.3.0-incubating")
    api("com.google.guava:guava:29.0-jre")
    api("com.google.protobuf:protobuf-java:3.11.4")

    // lombok support
    compileOnly("org.projectlombok:lombok:1.18.12")
    annotationProcessor("org.projectlombok:lombok:1.18.12")
    testCompileOnly("org.projectlombok:lombok:1.18.12")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.12")

    // testng
    testImplementation("org.testng:testng:6.8")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.apache.commons:commons-lang3:3.10")
}

sourceSets {

    main {
        java {
            srcDir("src/main/java")
            srcDir("src/main/resources")
        }
        proto {
            srcDir("proto/src")
        }
    }

    test {
        java.srcDir("src/test/java")
        java.srcDir("src/test/resources")
    }
}

val generatedDir = "$projectDir/generated"
protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.13.0"
    }
}

tasks.test {
    useTestNG()
    testLogging {
        testLogging.showStandardStreams = true
        failFast = true
        events("passed", "skipped", "failed")
    }
}

