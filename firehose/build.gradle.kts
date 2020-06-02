plugins {
    `java-library`
    `maven-publish`
    signing
    id("com.github.johnrengelman.shadow") version ("5.2.0")
}

group = "com.whylabs"
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

dependencies {
    implementation("org.slf4j:slf4j-api:1.7.27")
    implementation("com.amazonaws:aws-java-sdk-kinesis:1.11.769")

    // project dependencies
    implementation(project(":whylogs-java:core"))

    // lombok support
    compileOnly("org.projectlombok:lombok:1.18.12")
    annotationProcessor("org.projectlombok:lombok:1.18.12")
    testCompileOnly("org.projectlombok:lombok:1.18.12")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.12")

    // testng
    testImplementation("org.testng:testng:6.8")
    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.apache.commons:commons-lang3:3.10")
    testImplementation("com.google.guava:guava:29.0-jre")
}

sourceSets {
    main {
        java.srcDir("src/main/java")
        java.srcDir("src/main/resources")
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
