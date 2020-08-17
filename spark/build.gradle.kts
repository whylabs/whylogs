plugins {
    scala
    `java-library`
    id("com.github.maiflai.scalatest") version "0.26"
}

repositories {
    jcenter()
}

group = "com.whylabs"
version = rootProject.version

spotless {
    java {
        googleJavaFormat()
    }
}

val scalaVersion = project.properties.getOrDefault("scalaVersion", "2.11")
val sparkVersion = "2.4.5"
val artifactBaseName = "whylogs-spark_$scalaVersion"

tasks.jar {
    archiveBaseName.set(artifactBaseName)
}

fun scalaPackage(groupId: String, name: String, version: String) =
    "$groupId:${name}_$scalaVersion:$version"

dependencies {
    api("org.slf4j:slf4j-api:1.7.27")
    implementation(scalaPackage("org.apache.spark", "spark-core", sparkVersion))
    implementation(scalaPackage("org.apache.spark", "spark-sql", sparkVersion))

    // project dependencies
    implementation(project(":core"))

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

// expose only
configurations.create("jar")

artifacts {
    add("jar", tasks.jar)
}
