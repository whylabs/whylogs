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
    java
    signing
    `maven-publish`
}

apply(plugin = "com.google.protobuf")

group = rootProject.group
version = rootProject.version
val artifactBaseName = "${rootProject.name}-core"


java {
    withJavadocJar()
    withSourcesJar()
}

spotless {
    java {
        googleJavaFormat()
    }
}

dependencies {
    api("org.slf4j:slf4j-api:1.7.27")
    api("org.apache.datasketches:datasketches-java:1.3.0-incubating")
    api("org.apache.commons:commons-lang3:3.10")
    api("com.google.guava:guava:19.0")
    api("com.google.protobuf:protobuf-java:3.13.0")
    api("com.google.code.findbugs:jsr305:3.0.2")

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

val javadocJar by tasks

publishing {
    val ossrhUsername: String? by project
    val ossrhPassword: String? by project

    publications {
        val isSnapShot = version.toString().endsWith("SNAPSHOT")

        repositories {
            maven {
                val stagingRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")

                url = if (isSnapShot) snapshotsRepoUrl else stagingRepoUrl
                credentials {
                    username = ossrhUsername
                    password = ossrhPassword
                }
            }
        }

        create<MavenPublication>("mavenJava") {
            from(components["java"])

            artifactId = artifactBaseName
            groupId = project.group as String
            version = project.version as String
            description = "WhyLogs - a powerful data profiling library for your ML pipelines"

            pom {
                name.set("whylogs-core")
                description.set("The core library for WhyLogs")
                url.set("https://github.com/whylabs/whylogs-java")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("WhyLabs")
                        name.set("WhyLabs, Inc")
                        email.set("support@whylabs.ai")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/whylabs/whylogs-java.git")
                    developerConnection.set("scm:git:ssh://github.com/whylabs/whylogs-java.git")
                    url.set("https://github.com/whylabs/whylogs-java")
                }

            }
        }
    }
}

signing {
    setRequired({
        (rootProject.extra["isReleaseVersion"] as Boolean) && gradle.taskGraph.hasTask("uploadArchives")
    })
    sign(publishing.publications["mavenJava"])
}
