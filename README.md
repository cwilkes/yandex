Here's some code I'm developing for use with the [yandex clicklog](http://imat-relpred.yandex.ru/en/datasets) competition

The "avro" project is just there to create a jar with the [avro](http://avro.apache.org/) java class for use in Hadoop.

The "data" project is for python and java code to work on the data.  For example there's python code to read in the multiline clicklog file and combine all records
for a session into a single avro record.
