import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.jvm.tasks.Jar
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
    java
    idea
    application
    id("org.jetbrains.dokka") version "0.9.17"
    kotlin("jvm") version "1.3.10"
    id("nebula.dependency-lock") version "7.1.0"
    id("nebula.release") version "6.3.5"
    id("nebula.nebula-bintray") version "3.5.2"
}

group = "se.arbetsformedlingen"
version = "1.0-SNAPSHOT"

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

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClassName = "se.arbetsformedlingen.skolverket.MainKt"
}

tasks.withType<DokkaTask> {
    outputFormat = "html"
    outputDirectory = "$buildDir/javadoc"
}

val dokkaJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    classifier = "javadoc"
    from(tasks.withType<DokkaTask>())
}