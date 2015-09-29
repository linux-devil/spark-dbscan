package configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wlucia on 29/09/15.
 */
public class SparkFactory {

    private SparkConf conf;

    public SparkFactory() {
        conf = new SparkConf();
    }

    public SparkFactory setMaster(String master) {
        conf.setMaster(master);
        return this;
    }
    public JavaSparkContext sparkContext(String appName) {
        conf.setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

}
