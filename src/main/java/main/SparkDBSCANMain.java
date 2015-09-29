package main;

import bean.GeoPoint;
import clustering.SparkDBSCAN;
import indexing.KDTree;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wlucia on 29/09/15.
 */
public class SparkDBSCANMain {

    public static void main(String[] args) {
        if(args.length != 4) {
            System.out.println("Bad Parameters :'(");
            System.out.println("Usage: java -jar DistributedDbScan epsilon minPts inputFilePath inputFilePath");
            System.exit(-1);
        }

        long R = 6378388;

        double epsilon = Double.parseDouble(args[0]);
        int minPts = Integer.parseInt(args[1]);
        String inputPath = args[2];
        String outputPath = args[3];
        System.out.println(String.format("*** PARAMS:\nEpsilon -> %f\nMinPts -> %d\nInput -> %s\nOutput -> %s",
                epsilon,
                minPts,
                inputPath,
                outputPath
        ));


        JavaSparkContext sc = null;

        SparkDBSCAN dbScan = new SparkDBSCAN(sc, epsilon, minPts, inputPath, outputPath);
        dbScan.clustering();
    }

    private static synchronized void addPointToKdTree(GeoPoint p, KDTree<GeoPoint> pkdt) {
        pkdt.insert(new double[]{p.getLat(), p.getLng()}, p);
    }
    }

}
