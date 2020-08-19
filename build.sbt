name := "JSONtoCSV"

version := "0.1"

scalaVersion := "2.12.8" // don't use 2.12.3, throw Exceptions
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.9.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0"// % "runtime",

resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"
