import org.gradle.jvm.tasks.Jar
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
    `build-scan`
    application
    kotlin("jvm") version "1.3.10"
    id("org.jetbrains.dokka") version "0.9.17"
    id("nebula.dependency-lock") version "7.1.0"
    id("nebula.release") version "6.3.5"
    id("maven-publish")
}

group = "se.arbetsformedlingen"

repositories {
    jcenter()
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("com.google.code.gson:gson:2.8.5")
    compile("com.xenomachina:kotlin-argparser:2.0.7")
    compile(group = "org.apache.avro", name = "avro", version = "1.8.2")
}

application {
    mainClassName = "se.arbetsformedlingen.avro.MainKt"
}

// Configure existing Dokka task to output HTML to typical Javadoc directory
tasks.dokka {
    outputFormat = "html"
    outputDirectory = "$buildDir/javadoc"
}

// Create dokka Jar task from dokka task output
val dokkaJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    classifier = "javadoc"
    from(tasks.dokka)
}

// Create sources Jar from main kotlin sources
val sourcesJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles sources JAR"
    classifier = "sources"
    from(sourceSets["main"].allSource)
}

buildScan {
    termsOfServiceUrl = "https://gradle.com/terms-of-service"
    termsOfServiceAgree = "yes"

    publishAlways()
}

publishing {
    /*repositories {
        maven {
            url = uri("git@github.com:DanielOberg/AvroSchemaFromJsonExamples.git")
        }
    }*/
    publications {
        create<MavenPublication>("default") {
            from(components["java"])
            artifact(dokkaJar)
            artifact(sourcesJar)
        }
    }
}