import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
    dependencies {
        classpath("com.amazonaws:aws-java-sdk-core:1.11.766")
        classpath("com.github.jengelman.gradle.plugins:shadow:5.2.0")
    }
}

plugins {
    `java-library`
    signing
    `maven-publish`
    id("com.github.johnrengelman.shadow") version ("5.2.0")
}

val artifactBaseName = "whylogs-core-bundle"
val versionString = rootProject.version

group = rootProject.group
version = versionString

dependencies {
    compileOnly(project(":core"))
    compileOnly("com.google.protobuf:protobuf-java-util:3.20.1")
}

// Do not build the jar for this package
tasks.compileJava {
    enabled = false
}

tasks.build {
    dependsOn(shadowJar)
}

tasks.publish {
    dependsOn(shadowJar)
}

val shadowJar: ShadowJar by tasks
shadowJar.dependsOn(":core:build")
val artifactName = "${project.name}-${versionString}.jar"
shadowJar.apply {
    configurations[0] = project.configurations.compileClasspath.get()
    exclude("*.properties")
    exclude("*.proto")
    exclude("META-INF/*")
    dependencies {
        // double load logging jars is always a headache
        exclude(dependency("org.slf4j:slf4j-api"))
        // javax mismatch also causes headache
        exclude(dependency("javax.annotation:javax.annotation-api"))
        exclude(dependency("javax.activation:activation"))
        exclude(dependency("com.google.code.findbugs:jsr305"))

    }
    // relocate core libraries
    relocate("org.apache.datasketches", "com.shaded.whylabs.org.apache.datasketches")
    relocate("com.google", "com.shaded.whylabs.com.google")
    relocate("org.apache.commons", "com.shaded.whylabs.com.apache.commons")
    relocate("org.checkerframework", "com.shaded.whylabs.org.checkerframework")

    archiveFileName.set(artifactName)
}

artifacts {
    add("archives", shadowJar)
}


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
            artifact(shadowJar)

            artifactId = artifactBaseName
            groupId = project.group as String
            version = project.version as String
            description = "WhyLogs - a powerful data profiling library for your ML pipelines"

            pom {
                name.set(artifactBaseName)
                description.set("The core bundle for whylogs")
                url.set("https://github.com/whylabs/whylogs")
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
                    connection.set("scm:git:git://github.com/whylabs/whylogs.git")
                    developerConnection.set("scm:git:ssh://github.com/whylabs/whylogs.git")
                    url.set("https://github.com/whylabs/whylogs")
                }

            }
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    setRequired({
        (rootProject.extra["isReleaseVersion"] as Boolean) && gradle.taskGraph.hasTask("uploadArchives")
    })
    sign(publishing.publications["mavenJava"])
}
