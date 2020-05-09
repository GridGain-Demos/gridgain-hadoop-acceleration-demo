/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/**
 *
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
