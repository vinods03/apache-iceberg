Important concepts around Apache iceberg (like COW, MOR, hidden partitioning) and sample implementation of a data lakehouse project using Iceberg are shown here.

**Apache Iceberg on Athena - Handson 1:** shows the metadata files involved in an Iceberg table.
**Apache Iceberg on Athena - Handson 2:** shows the working of update & delete in an Iceberg table.
**Apache Iceberg on Athena - Handson 3:** is similar to the previous one, except that here, we dont have to clear the source table after every target table load. The select query inside the merge statement picks the latest record for every id. So here, source will have duplicates but not the target.
**Apache Iceberg on Athena - Handson 4:** is same as above, except that this is a python implementation using Glue Python shell job. In a data lakehouse project, this Glue Pythion shell job will be triggered using an Event Bridge Rule or Lambda/SNS whenever the source S3 layer is populated.
