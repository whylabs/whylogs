plugins {
    application
    `maven-publish`
    signing
    id("com.github.johnrengelman.shadow") version ("5.2.0")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

group = rootProject.group
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

application {
    applicationName = "profiler"
    mainClassName = "com.whylogs.cli.Profiler"
}


dependencies {
    implementation(project(":core"))

    implementation("org.slf4j:slf4j-api:1.7.27")
    implementation("org.apache.logging.log4j:log4j-core:2.13.2")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.2")

    implementation("org.apache.commons:commons-csv:1.8")
    implementation("commons-io:commons-io:2.6")
    implementation("org.apache.commons:commons-lang3:3.10")

    implementation("com.google.protobuf:protobuf-java-util:3.11.4")
    implementation("info.picocli:picocli:4.2.0")

    // lombok support
    compileOnly("org.projectlombok:lombok:1.18.12")
    annotationProcessor("org.projectlombok:lombok:1.18.12")
    annotationProcessor("info.picocli:picocli:4.2.0")

    testCompileOnly("org.projectlombok:lombok:1.18.12")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.12")

    // testng
    testImplementation("org.testng:testng:6.8")
}

tasks.compileJava {
    //enable compilation in a separate daemon process
    options.fork()
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
