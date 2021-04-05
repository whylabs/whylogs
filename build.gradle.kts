plugins {
    `java-library`
    idea
    id("com.diffplug.gradle.spotless") version ("3.28.1") apply false
}

group = "ai.whylabs"
version = "0.1.2-b6"
//version = "0.1.2-b6-${project.properties.getOrDefault("versionType", "SNAPSHOT")}"
extra["isReleaseVersion"] = !version.toString().endsWith("SNAPSHOT")

allprojects {
    version = version
    group = group

    apply(plugin = "idea")
    apply(plugin = "java")
    val gitlabMavenToken = System.getenv("MAVEN_TOKEN")

    repositories {
        mavenCentral()
        maven {
            // https://gitlab.com/whylabs/core/songbird-java-client/-/packages
            url = uri("https://gitlab.com/api/v4/projects/22420498/packages/maven")
            name = "Gitlab"


            val headerName = if (System.getenv("CI_JOB_STAGE").isNullOrEmpty()) "Private-Token" else "Job-Token"
            credentials(HttpHeaderCredentials::class) {
                name = headerName
                value = gitlabMavenToken
            }

            authentication {
                create<HttpHeaderAuthentication>("header")
            }
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}

subprojects {
    apply(plugin = "com.diffplug.gradle.spotless")
}
