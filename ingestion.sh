rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/2021-informe-ministerio.csv
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/202206-informe-ministerio.csv
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/aeropuertos_detalle.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest

