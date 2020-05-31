tasks.create("build-python") {
    project.exec {
        commandLine = "python setup.py build --verbose".split(" ")
    }
}

tasks.compileJava {
    enabled = false
    options.isIncremental = false
}
