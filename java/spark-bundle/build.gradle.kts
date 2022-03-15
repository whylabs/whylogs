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

val scalaVersion = project.properties.getOrDefault("scalaVersion", "2.12")
val sparkVersion = project.properties.getOrDefault("sparkVersion", "3.1.1") as String
val artifactBaseName = "whylogs-spark-bundle_${sparkVersion}-scala_$scalaVersion"
val versionString = rootProject.version

group = rootProject.group
version = versionString

dependencies {
    implementation(project(":core"))

    // we only depends on the output of the whylogs-spark components
    // we don't want to pull in Spark dependencies here
    implementation(project(":spark", "jar"))
    implementation("ai.whylabs:whylabs-api-client:0.1.7")
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
shadowJar.dependsOn(":spark:build")
shadowJar.apply {
    exclude("*.properties")
    exclude("META-INF/*")
    dependencies {
        // double load logging jars is always a headache
        exclude(dependency("org.slf4j:slf4j-api"))
        // javax mismatch also causes headache
        exclude(dependency("javax.annotation:javax.annotation-api"))
    }
    // relocate core libraries
    relocate("org.apache.datasketches", "com.shaded.whylabs.org.apache.datasketches")
    relocate("com.google", "com.shaded.whylabs.com.google")
    relocate("org.checkerframework", "com.shaded.whylabs.org.checkerframework")

    // okio is consumed by songbird
    relocate("okio", "com.shaded.whylabs.okio")
    relocate("okhttp3", "com.shaded.whylabs.okhttp3")

    archiveFileName.set("$artifactBaseName-${versionString}.jar")
}

artifacts {
  add ("archives", shadowJar)
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
                name.set("whylogs-spark-bundle")
                description.set("spark bundle library for WhyLogs")
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
