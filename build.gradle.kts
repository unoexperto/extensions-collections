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
    kotlin("jvm") version "1.3.61"
    id("java")
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
project.version = "1.7"
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

dependencies {
    compile(kotlin("stdlib-jdk8", kotlinVersion))
    compile(kotlin("reflect", kotlinVersion))

    compileOnly("org.fusesource.leveldbjni:leveldbjni-all:1.8")
    compileOnly("org.rocksdb:rocksdbjni:6.6.4")
    compileOnly("io.netty:netty-buffer:4.1.45.Final")

    testCompile(kotlin("test-junit5", kotlinVersion))
    testCompile("org.junit.jupiter:junit-jupiter:5.6.0")
    testCompile("org.fusesource.leveldbjni:leveldbjni-all:1.8")
    testCompile("org.rocksdb:rocksdbjni:6.6.4")
    testCompile("io.netty:netty-buffer:4.1.45.Final")

//    api("junit:junit:4.12")
//    implementation("junit:junit:4.12")
//    testImplementation("junit:junit:4.12")
}

repositories {
    mavenCentral()
    jcenter()
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

    withType(Test::class.java) {
        testLogging.showStandardStreams = true
        testLogging.showExceptions = true
        useJUnitPlatform {
        }
    }
}
