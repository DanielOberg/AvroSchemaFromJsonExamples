import com.jfrog.bintray.gradle.BintrayExtension
import org.gradle.jvm.tasks.Jar
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.dokka.gradle.LinkMapping

plugins {
    `build-scan`
    application
    kotlin("jvm") version "1.3.10"
    id("org.jetbrains.dokka") version "0.9.17"
    id("nebula.dependency-lock") version "7.1.0"
    id("nebula.release") version "6.3.5"
    id("com.jfrog.bintray") version "1.8.4"
    id("maven-publish")
}

val githubRepoUrl = "https://github.com/DanielOberg/AvroSchemaFromJsonExamples/"
group = "se.arbetsformedlingen"

repositories {
    jcenter()
    maven { url = uri("https://packages.confluent.io/maven/") }
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("com.google.code.gson:gson:2.8.5")
    compile("com.xenomachina:kotlin-argparser:2.0.7")
    compile("org.jetbrains.dokka:dokka-gradle-plugin:0.9.9")
    compile(group = "org.apache.avro", name = "avro", version = "1.8.2")
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.2")
    testCompile("org.junit.jupiter:junit-jupiter-params:5.3.2")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.2")
    testCompile("org.apache.kafka:kafka-clients:2.1.0")
    testCompile(group = "io.confluent", name = "kafka-avro-serializer", version = "5.0.1")
    testCompile(group = "org.slf4j", name = "slf4j-simple", version = "1.7.25")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
    }
}

tasks.withType<Wrapper> {
    gradleVersion = "4.8"
}

application {
    mainClassName = "se.arbetsformedlingen.avro.MainKt"
}

// Configure existing Dokka task to output HTML to typical Javadoc directory
tasks.dokka {
    outputFormat = "html"
    outputDirectory = "$buildDir/javadoc"
}

val dokka by tasks.getting(DokkaTask::class) {
    val src = "src/main/kotlin"

    includes = listOf("README.md")
    val mapping = LinkMapping().apply {
        dir = src
        url = "${githubRepoUrl}/blob/master/$src"
        suffix = "#L"
    }
    linkMappings = arrayListOf(mapping)
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
    publications {
        create<MavenPublication>("default") {
            from(components["java"])
            artifact(dokkaJar)
            artifact(sourcesJar)
        }
    }
    repositories {
        maven {
            url = uri("https://dl.bintray.com/arbetsformedlingen/avro")
            credentials {
                username = System.getenv("BINTRAY_USER")
                password = System.getenv("BINTRAY_KEY")
            }
        }
    }
}

bintray {
    user = System.getenv("BINTRAY_USER")
    key = System.getenv("BINTRAY_KEY")
    publish = true
    setPublications("default")
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "avro"
        name = "AvroSchemaFromJsonExamples"
        userOrg = "arbetsformedlingen"
        websiteUrl = "https://github.com/DanielOberg/AvroSchemaFromJsonExamples"
        githubRepo = "DanielOberg/AvroSchemaFromJsonExamples"
        vcsUrl = "https://github.com/DanielOberg/AvroSchemaFromJsonExamples"
        issueTrackerUrl = "https://github.com/DanielOberg/AvroSchemaFromJsonExamples/issues"
        description = "Creates an Avro Schema from a bunch of JSON examples"
        setLabels("kotlin")
        setLicenses("MIT")
        desc = description
    })
}