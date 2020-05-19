/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.dla.sparkloader;

import org.apache.spark.SparkConf;
import org.gridgain.sparkloader.GridGainSparkLoader;

public class HadoopSparkJSON {

	public static void main(String[] args) {
        if (args.length < 2 || args.length > 3)
            throw new IllegalArgumentException("You should set the path to client configuration file and path to JSON file.");

        String configPath = args[0];

        String personCsvPath = args[1];

        Boolean advanced = args.length == 3 && args[2].contains("true");

        GridGainSparkLoader sparkLoader;

        if (advanced) {
            //you can set different options using spark configuration. Not only that you can find in GridGainSparkLoaderBuilder.
            SparkConf sparkConf = new SparkConf()
                    .setAppName("LoadingFromJsonExample") //comment this line in case if you are going to use spark submit with application name option
                    .setMaster("local") //comment this line in case if you are going to use spark submit with master option
                    .set("spark.hadoop.dfs.client.use.datanode.hostname", "true");

            //Master and appName GridGainSparkLoaderBuilder options related to Spark will be ignored.
            sparkLoader = new GridGainSparkLoader.GridGainSparkLoaderBuilder()
                    .buildFromSparkConfig(sparkConf, configPath);
        } else
            sparkLoader = new GridGainSparkLoader.GridGainSparkLoaderBuilder()
                    .setApplicationName("LoadingFromJsonExample") //comment this line in case if you are going to use spark submit with application name option
                    .setMaster("local") //comment this line in case if you are going to use spark submit with master option
                    .build(configPath);

        sparkLoader.loadFromJsonToExistingCache(
                personCsvPath,
                "Person")
                .filter("company = 'bank'")
                .save();

        sparkLoader.loadFromJsonToNewCache(
                personCsvPath,
                "NotBankPersonWithoutAge",
                "id, city_id",
                "template=partitioned,backups=1")
                .select("id", "name", "city_id", "company")
                .filter("company != 'bank'")
                .save();

        sparkLoader.closeSession();
    }

}
