name := "MovieSimilarities1MDataset"

version := "1.0"

organization := "Movies_Recommendation"
scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
"org.apache.spark" %% "spark-sql" % "3.5.5" % "provided"
)



