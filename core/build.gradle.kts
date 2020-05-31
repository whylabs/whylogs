import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

buildscript {
    dependencies {
        classpath("com.amazonaws:aws-java-sdk-core:1.11.766")
    }
}

plugins {
    `java-library`
    `maven-publish`
    idea
}

group = "com.whylabs"
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

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.whylabs"
            artifactId = "whylogs-java"
            version = version
            from(components["java"])
            pom {
                name.set("WhyLogs")
                description.set("A lightweight data profiling library")
                url.set("http://www.whylogs.ai")
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
                        email.set("info@whylabs.ai")
                    }
                }
                scm {
                    url.set("http://whylabs.ai/")
                }
            }
        }

        repositories {
            val isSnapShot = version.toString().endsWith("SNAPSHOT")
            maven {
                // change URLs to point to your repos, e.g. http://my.org/repo
                val releasesRepoUrl = uri("$buildDir/repos/releases")
                val snapshotsRepoUrl = uri("$buildDir/repos/snapshots")
                url = if (isSnapShot) snapshotsRepoUrl else releasesRepoUrl

            }
            maven {
                // TODO: change this URL base on the stage of publishing?
                val s3Base = "s3://whylabs-andy-maven-us-west-2/repos"
                val releasesRepoUrl = uri("$s3Base/releases")
                val snapshotsRepoUrl = uri("$s3Base/snapshots")
                url = if (isSnapShot) snapshotsRepoUrl else releasesRepoUrl

                // set up AWS authentication
                val credentials = DefaultAWSCredentialsProviderChain.getInstance().credentials
                credentials(AwsCredentials::class) {
                    accessKey = credentials.awsAccessKeyId
                    secretKey = credentials.awsSecretKey
                    // optional
                    if (credentials is com.amazonaws.auth.AWSSessionCredentials) {
                        sessionToken = credentials.sessionToken
                    }
                }
            }
        }
    }
}

