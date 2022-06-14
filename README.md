# Anton_Slizh_M06_SparkBasics_JVM_AZURE
## Check <i>m06_sparkbasics_jvm_azure_report.docx</i> file to see all steps for set up environment and run given application
# Task
* Create Spark etl job to read data from storage container.
* Check hotels data on incorrect (null) values (Latitude & Longitude). For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
* Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)
* Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
* Development and testing is recommended to do locally in your IDE environment.
* Store enriched data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy).