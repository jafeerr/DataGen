name := "SchemaTestDataGenerator"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies++=Seq("org.apache.spark"%%"spark-sql"%"2.2.1","org.apache.spark"%%"spark-core"%"2.2.1","com.typesafe" % "config" % "1.2.1")
