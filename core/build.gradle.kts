import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

buildscript {
    dependencies {
        classpath("com.amazonaws:aws-java-sdk-core:1.11.766")
    }
}

plugins {
    `java-library`
}

group = rootProject.group
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

dependencies {
    api(project(":proto"))
    api("org.slf4j:slf4j-api:1.7.27")
    api("org.apache.datasketches:datasketches-java:1.3.0-incubating")
    api("com.google.guava:guava:29.0-jre")

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
    }

    test {
        java.srcDir("src/test/java")
        java.srcDir("src/test/resources")
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

