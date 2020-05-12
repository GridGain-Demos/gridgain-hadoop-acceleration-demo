/*
   Copyright 2020 GridGain Systems

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.gridgain.dla.sparkloader;
import org.apache.spark.SparkConf;
import org.gridgain.sparkloader.GridGainSparkLoader;


/**
 * @author rdharampal
 *
 */
public class HadoopSparkCSV {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        if (args.length < 2 || args.length > 3)
            throw new IllegalArgumentException("You should set the path to client configuration file and path to CSV file.");

        String configPath = args[0];

        String personCsvPath = args[1];

        Boolean advanced = args.length == 3 && args[2].contains("true");

        GridGainSparkLoader sparkLoader;

        if (advanced) {
            SparkConf sparkConf = new SparkConf()
                    .setAppName("LoadingFromCsvExample") //comment this line in case if you are going to use spark submit with application name option
                    .setMaster("local") //comment this line in case if you are going to use spark submit with master option
                    .set("spark.hadoop.dfs.client.use.datanode.hostname", "true");

            //Master and appName GridGainSparkLoaderBuilder options related to Spark will be ignored.
            sparkLoader = new GridGainSparkLoader.GridGainSparkLoaderBuilder()
                    .buildFromSparkConfig(sparkConf, configPath);
        } else
            sparkLoader = new GridGainSparkLoader.GridGainSparkLoaderBuilder()
                    .setApplicationName("LoadingFromCsvExample") //comment this line in case if you are going to use spark submit with application name option
                    .setMaster("local") //comment this line in case if you are going to use spark submit with master option
                    .build(configPath);

        sparkLoader.loadFromCsvToExistingCache(
                personCsvPath,
                "Person",
                true,
                ",")
                .filter("company = 'bank'")
                .save();

        sparkLoader.loadFromCsvToNewCache(
                personCsvPath,
                "NotBankPersonWithoutAge",
                true,
                ",",
                "id, city_id",
                "template=partitioned,backups=1")
                .select("id", "name", "city_id", "company")
                .filter("company != 'bank'")
                .save();

        sparkLoader.closeSession();
     }

}
