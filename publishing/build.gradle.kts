import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import groovy.util.Node
import groovy.util.NodeList

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

val scalaVersion = project.properties.getOrDefault("scalaVersion", "2.11")
val artifactBaseName = "whylogs-spark-bundle_$scalaVersion"
val versionString = rootProject.version

group = "com.whylabs"
version = versionString

dependencies {
    implementation(project(":whylogs-java:core"))

    // we only depends on the output of the whylogs-spark components
    // we don't want to pull in Spark dependencies here
    implementation(project(":whylogs-java:spark", "jar"))
}

// Do not build the jar for this package
tasks.compileJava {
    enabled = false
}

val shadowJar: ShadowJar by tasks
shadowJar.apply {
    exclude("*.properties")
    exclude("META-INF/*")
    dependencies {
        exclude(dependency("org.slf4j:slf4j-api"))
        exclude(dependency("javax.annotation:javax.annotation-api"))
    }
    relocate("org.apache.datasketches", "com.shaded.whylabs.org.apache.datasketches")
    relocate("com.google", "com.shaded.whylabs.com.google")
    relocate("org.checkerframework", "com.shaded.whylabs.org.checkerframework")
    archiveFileName.set("$artifactBaseName-${versionString}.jar")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.whylabs"
            artifactId = artifactBaseName
            version = version
            artifact(shadowJar) {
                classifier = "" // why do i need this GRADLE ?
            }
            project.shadow.component(this)

            pom {
                name.set("WhyLogs-Spark-Bundle")
                description.set("A single jar to easily deploy WhyLogs to Spark")
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

                // rewrite XML dependencies blob to only include SLF4J
                withXml {
                    val dependencies =
                        (asNode()["dependencies"] as NodeList)[0] as Node
                    val childNodes = dependencies.children().filterIsInstance<Node>()
                        .filter { (it.name() as groovy.xml.QName).qualifiedName == "dependency" }

                    // remove dependencies that are not slf4j
                    val dependenciesToBeRemoved = childNodes.filterNot {
                        val node =
                            (it.get("groupId") as NodeList)[0] as Node
                        val groupId = (node.value() as NodeList)[0] as String

                        groupId.startsWith("org.slf4j")
                    }

                    dependenciesToBeRemoved.forEach {
                        dependencies.remove(it)
                    }
                }
            }
        }

        repositories {
            val isSnapShot = version.toString().endsWith("SNAPSHOT")
            maven {
                // change URLs to point to your repos, e.g. http://my.org/repo
                val releasesRepoUrl = uri("${rootProject.buildDir}/repos/releases")
                val snapshotsRepoUrl = uri("${rootProject.buildDir}/repos/snapshots")
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

