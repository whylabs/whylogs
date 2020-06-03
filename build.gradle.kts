buildscript {
    repositories {
        jcenter()
    }
}

plugins {
    java
}

idea {
    module {
        sourceDirs = setOf(file("src"))
        testSourceDirs = setOf(file("tests"))
        excludeDirs = excludeDirs + file(".eggs") + file(".pytests_cache") + file("build") + file("dist")
    }
}

group = "com.whylabs"

tasks.register<Copy>("copy-proto") {
    dependsOn(":proto:build")

    val protoPath = projectDir.resolve("src/whylabs/logs/proto")

    val pythonPath = project(":proto").projectDir.resolve("generated/main/python")
    from("$pythonPath")
    include("*.py")
    into(protoPath)

    // From this discussion https://github.com/protocolbuffers/protobuf/issues/1491
    // somehow the "fix" is no longer present in the current code base
    // See current: https://git.io/JfPeA
    val pattern = "import (.+_pb2)".toRegex()
    filter { line ->
        if (pattern.matchEntire(line) != null) {
            line.replace("import", "from . import")
        } else {
            line
        }
    }
}

tasks.register<Exec>("pip-install") {
    commandLine = "pip install -v -e .[dev]".split(" ")
}

tasks.register<Exec>("test-python") {
    commandLine = "python setup.py test".split(" ")
}

tasks.register<Exec>("build-python") {
    dependsOn("copy-proto")
    dependsOn("pip-install")
    dependsOn("test-python")

    commandLine = "python setup.py build --verbose".split(" ")
}

tasks.compileJava {
    enabled = false
}
