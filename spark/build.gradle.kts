plugins {
    scala
    `java-library`
    signing
    `maven-publish`
    id("com.github.maiflai.scalatest") version "0.26"
}

repositories {
    jcenter()
}

group = rootProject.group
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

val scalaVersion = project.properties.getOrDefault("scalaVersion", "2.12")
val sparkVersion = project.properties.getOrDefault("sparkVersion", "3.1.1") as String
val artifactBaseName = "${rootProject.name}-spark_$sparkVersion-scala_$scalaVersion"

tasks.jar {
    archiveBaseName.set(artifactBaseName)
}

fun scalaPackage(groupId: String, name: String, version: String) =
    "$groupId:${name}_$scalaVersion:$version"

sourceSets {
    main {
        withConvention(ScalaSourceSet::class) {
            scala {
                srcDirs(
                    listOf(
                        "${project.projectDir}/src/main/scala"
                    )
                )
            }
        }

        resources {
            includes.add("python/whyspark/__init__.py")
        }
    }
}

val scaladoc: ScalaDoc by tasks

val javadocJar by tasks.creating(Jar::class) {
    archiveBaseName.set("${rootProject.name}-spark")
    archiveClassifier.set("javadoc")
    dependsOn(scaladoc)
    from(scaladoc.destinationDir)
}

val sourcesJar by tasks.creating(Jar::class) {
    archiveBaseName.set("${rootProject.name}-spark")
    archiveClassifier.set("sources")
    from(sourceSets["main"].allSource)
}


artifacts {
    add("archives", sourcesJar)
    add("archives", javadocJar)
}

dependencies {
    api("org.slf4j:slf4j-api:1.7.27")
    implementation(scalaPackage("org.apache.spark", "spark-core", sparkVersion))
    implementation(scalaPackage("org.apache.spark", "spark-sql", sparkVersion))

    // project dependencies
    implementation(project(":core"))

    // Songbird
    implementation("ai.whylabs:whylabs-api-client:0.1.2")
    implementation("com.squareup.okhttp3:okhttp:4.9.1")

    // lombok support
    compileOnly("org.projectlombok:lombok:1.18.12")
    annotationProcessor("org.projectlombok:lombok:1.18.12")
    testCompileOnly("org.projectlombok:lombok:1.18.12")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.12")

    // testng
    testImplementation("org.testng:testng:6.8")
    testImplementation(scalaPackage("org.scalatest", "scalatest", "3.1.2"))
    testRuntimeOnly("org.slf4j:slf4j-log4j12:1.7.30")
    testRuntimeOnly("com.vladsch.flexmark:flexmark-profile-pegdown:0.36.8")
}

tasks.test {
    useTestNG()
    jvmArgs("-Dlog4j.configuration=file://${projectDir}/configurations/log4j.properties")
    testLogging {
        testLogging.showStandardStreams = true
        failFast = true
        events("passed", "skipped", "failed")
    }
}

tasks.jar {
    into("whyspark/") {
        from("python/whyspark/")
    }
}
// expose only
configurations.create("jar")

artifacts {
    add("jar", tasks.jar)
}

publishing {
    val ossrhUsername: String? by project
    val ossrhPassword: String? by project

    publications {
        repositories {
            maven {
                url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = ossrhUsername
                    password = ossrhPassword
                }
            }
        }

        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)

            artifactId = artifactBaseName
            groupId = project.group as String
            version = project.version as String
            description = "WhyLogs - a powerful data profiling library for your ML pipelines"

            pom {
                name.set("whylogs-spark")
                description.set("Spark integration for WhyLogs")
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
                        email.set("info@whylabs.ai")
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
