# GridGain Data Lake Accelerator Demo

Please visit my blog for [details-link](https://drive.google.com/file/d/1DvC5AwlnOI8d9TNOJVYnnCnkaojiGnKv/view?usp=sharing) of setup.

```
To run the HadoopSparkCSV SparkLoader:
c:\Work\Demos\gg-dla-demo>
java -cp ".\target\libs\*;.\target\gridgain-dla-demo-1.0.0.jar" org.gridgain.dla.sparkloader.HadoopSparkCSV .\config\client.xml hdfs://sandbox.hortonworks.com:8020/user/maria_dev/person.csv true

```
```
To run the HadoopSparkJSON SparkLoader:
c:\Work\Demos\gg-dla-demo>
java -cp ".\target\libs\*;.\target\gridgain-dla-demo-1.0.0.jar" org.gridgain.dla.sparkloader.HadoopSparkJSON .\config\client.xml hdfs://sandbox.hortonworks.com:8020/user/maria_dev/person.json true
```