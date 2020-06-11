//import com.github.jengelman.gradle.plugins.shadow.ShadowApplicationPlugin
//import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.publish.maven.MavenPom
import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.jfrog.bintray.gradle.BintrayExtension
import java.util.Date
import java.io.FileInputStream
import java.util.Properties

val kotlinVersion = plugins.getPlugin(KotlinPluginWrapper::class.java).kotlinPluginVersion

plugins {
    kotlin("jvm") version "1.3.72"
    id("java")
    idea
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("maven-publish")
    id("com.jfrog.bintray") version "1.8.4"
    `java-library`
}

kotlin {
    //    experimental.coroutines = Coroutines.ENABLE
}

val publicationName = "DefaultPublication"

project.group = "com.walkmind.extensions"
val artifactID = "collections"
project.version = "1.16"
val licenseName = "Apache-2.0"
val licenseUrl = "http://opensource.org/licenses/apache-2.0"
val repoHttpsUrl = "https://github.com/unoexperto/extensions-collections.git"
val repoSshUri = "git@github.com:unoexperto/extensions-collections.git"
val (bintrayUser, bintrayKey) = loadBintrayCredentials()

fun loadBintrayCredentials(): Pair<String, String> {
    val path = "${System.getProperty("user.home")}/.bintray/.credentials"
    val fis = FileInputStream(path)
    val prop = Properties()
    prop.load(fis)
    return prop.getProperty("user") to prop.getProperty("password")
}

bintray {
    user = bintrayUser
    key = bintrayKey
    publish = true
//    dryRun = true

    setPublications(publicationName)

    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "maven"
        name = "extensions-collections"
        userOrg = "cppexpert"
        setLicenses(licenseName)
        vcsUrl = repoHttpsUrl
        setLabels("kotlin")

        version(delegateClosureOf<BintrayExtension.VersionConfig> {
            name = project.version as? String
            released = Date().toString()
            desc = project.description
//            attributes = mapOf("attrName" to "attrValue")
        })
    })
}

val sourcesJar by tasks.creating(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

val javadocJar by tasks.creating(Jar::class) {
    archiveClassifier.set("javadoc")
    from("$buildDir/javadoc")
}

val jar = tasks["jar"] as org.gradle.jvm.tasks.Jar

fun MavenPom.addDependencies() = withXml {
    asNode().appendNode("dependencies").let { depNode ->
        configurations.implementation.get().allDependencies.forEach {
            depNode.appendNode("dependency").apply {
                appendNode("groupId", it.group)
                appendNode("artifactId", it.name)
                appendNode("version", it.version)
            }
        }
    }
}

publishing {
    publications {
        create(publicationName, MavenPublication::class) {
            artifactId = artifactID
            groupId = project.group.toString()
            version = project.version.toString()
            description = project.description

            artifact(jar)
            artifact(sourcesJar) {
                classifier = "sources"
            }
            artifact(javadocJar) {
                classifier = "javadoc"
            }
            pom.addDependencies()
            pom {
                packaging = "jar"
                developers {
                    developer {
                        email.set("unoexperto.support@mailnull.com")
                        id.set("unoexperto")
                        name.set("ruslan")
                    }
                }
                licenses {
                    license {
                        name.set(licenseName)
                        url.set(licenseUrl)
                        distribution.set("repo")
                    }
                }
                scm {
                    connection.set("scm:$repoSshUri")
                    developerConnection.set("scm:$repoSshUri")
                    url.set(repoHttpsUrl)
                }
            }
        }
    }
}

idea {
    module {
        isDownloadJavadoc = true
        isDownloadSources = true
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8", kotlinVersion))
    implementation(kotlin("reflect", kotlinVersion))

    compileOnly("org.fusesource.leveldbjni:leveldbjni-all:1.8")
    compileOnly("org.rocksdb:rocksdbjni:6.8.1")
    compileOnly("io.netty:netty-buffer:4.1.50.Final")

    testImplementation(kotlin("test-junit5", kotlinVersion))
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
    testImplementation("net.jqwik:jqwik:1.3.0")
    testImplementation("org.fusesource.leveldbjni:leveldbjni-all:1.8")
    testImplementation("org.rocksdb:rocksdbjni:6.8.1")
    testImplementation("io.netty:netty-buffer:4.1.50.Final")
}

repositories {
    mavenCentral()
    jcenter()
    maven ("https://dl.bintray.com/kotlin/kotlin-eap")
    maven ("https://repo.spring.io/snapshot")
    maven ("https://repo.spring.io/release")
    flatDir {
        dirs("libs")
    }
}

configurations {
    implementation {
        resolutionStrategy.failOnVersionConflict()
    }
}

//sourceSets {
//    main {
//        java.srcDir("src/core/java")
//    }
//}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
        kotlinOptions.freeCompilerArgs = listOf(
                "-Xjsr305=strict",
                "-Xjvm-default=enable",
                "-XXLanguage:+NewInference",
                "-Xinline-classes",
                "-Xjvm-default=enable")
        kotlinOptions.apiVersion = "1.3"
        kotlinOptions.languageVersion = "1.3"
    }

    withType<Test>().all {
//        jvmArgs = listOf("--enable-preview")
        testLogging.showStandardStreams = true
        testLogging.showExceptions = true
        useJUnitPlatform {
        }
    }

    withType<JavaExec>().all {
//        jvmArgs = listOf("--enable-preview")
    }

    withType<Wrapper>().all {
        gradleVersion = "6.4.1"
        distributionType = Wrapper.DistributionType.BIN
    }

    withType<JavaCompile>().all {
//        options.compilerArgs.addAll(listOf("--enable-preview", "-Xlint:preview"))
    }
}
