addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
addSbtPlugin("com.github.tmtsoftware" % "kotlin-plugin" % "2.0.1-RC1")
addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.4")

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("twtmt", "sbt-plugins")
