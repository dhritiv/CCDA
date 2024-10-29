mvn clean package

bin/beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g ProjectA4-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2
bin/beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g ProjectA4-1.0-SNAPSHOT.jar Chicago_Crimes_10k.csv.bz2
bin/beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g ProjectA4-1.0-SNAPSHOT.jar Chicago_Crimes_100k.csv.bz2

spark-submit --conf spark.executor.memory=32g --conf spark.driver.memory=32g --master "local[*]" --class edu.ucr.cs.cs167.kwu116.BeastScalaSpatialAnalysis ./target/ProjectA4-1.0-SNAPSHOT.jar

spark-submit --conf spark.executor.memory=32g --conf spark.driver.memory=32g --master "local[*]" --class edu.ucr.cs.cs167.kwu116.BeastScalaTemporalAnalysis ./target/ProjectA4-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet 01/01/2010 01/01/2020

spark-submit --conf spark.executor.memory=32g --conf spark.driver.memory=32g --master "local[*]" --class edu.ucr.cs.cs167.kwu116.App ./target/ProjectA4-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet