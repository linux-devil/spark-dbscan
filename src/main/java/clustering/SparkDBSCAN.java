package clustering;

import bean.VisitBean;
import indexing.KDTree;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.impl.EdgeRDDImpl;
import org.apache.spark.graphx.impl.GraphImpl;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by wlucia on 29/09/15.
 */
public class SparkDBSCAN {
    private double epsilon;
    private int minPts;
    private String inputFile;
    private String outputFolder;
    private transient JavaSparkContext sc;

    public SparkDBSCAN(JavaSparkContext sc, double epsilon, int minPts, String inputFile, String outputFolder) {
        this.sc = sc;
        this.epsilon = epsilon;
        this.minPts = minPts;
        this.inputFile = inputFile;
        this.outputFolder = outputFolder;
    }

    private static synchronized void addPointToKdTree(VisitBean sb, KDTree<VisitBean> kdt) {
        kdt.insert(new double[]{sb.getLat(), sb.getLng()}, sb);
    }


    public void clustering(){
        JavaRDD<String> rows = sc.textFile(inputFile);

        AtomicLong idGen = new AtomicLong(0);

        Broadcast<AtomicLong> bgen = sc.broadcast(idGen);

        KDTree<VisitBean> pkdt = new KDTree<>(2);
        Broadcast<KDTree<VisitBean>> bpkdt = sc.broadcast(pkdt);
        System.out.println("*** GENERATING POINTS");

        // generating single points
        JavaRDD<Tuple2<Object, VisitBean>> points = rows.map(r -> {
            String[] fields = r.split(",");
            VisitBean sb = new VisitBean();
            sb.setId(Long.parseLong(fields[0]));
            sb.setLat(Double.parseDouble(fields[1]));
            sb.setLng(Double.parseDouble(fields[2]));
            addPointToKdTree(sb, bpkdt.getValue());
            return new Tuple2(sb.getId(), sb);
        });

        System.out.println(String.format("There are %d points.", points.count()));

        JavaRDD<Edge<VisitBean>> edges = points.flatMap(p -> {
            VisitBean spb = p._2();
            List<VisitBean> nn = bpkdt.getValue().ballSearch(new double[]{spb.getLat(), spb.getLng()}, epsilon);
            if (nn.size() >= minPts - 1) { // skip my self
                return nn.stream()
                        .map(t -> new Edge<>(spb.getId(), t.getId(), spb))
                        .collect(Collectors.toList());
            }else{
                ArrayList<Edge<VisitBean>> loop = new ArrayList<>();
                loop.add(new Edge<>(spb.getId(), spb.getId(), spb));
                return loop;
            }
        });

        EdgeRDDImpl<VisitBean, Long> e = EdgeRDD.fromEdges(edges.rdd(), scala.reflect.ClassTag$.MODULE$.apply(VisitBean.class), scala.reflect.ClassTag$.MODULE$.apply(Long.class));
        VertexRDD<VisitBean> v = VertexRDD.apply(points.rdd(), (EdgeRDD) e, null, scala.reflect.ClassTag$.MODULE$.apply(VisitBean.class));

        GraphImpl g = GraphImpl.apply(v, e,
                scala.reflect.ClassTag$.MODULE$.apply(VisitBean.class),
                scala.reflect.ClassTag$.MODULE$.apply(Long.class));

        Graph cc = g.ops().connectedComponents();
        long ms = System.currentTimeMillis();

        System.out.println("*** See file with timestamp: " + ms);

        cc.edges().toJavaRDD().map(t -> {
            Edge dt = (Edge)t;
            return String.format("%d,%d", dt.srcId(), dt.dstId());
        }).distinct().saveAsTextFile(outputFolder + "/cc_nodes_" + ms + ".csv");

        JavaPairRDD<Long, Edge<VisitBean>> ccTriplets = cc.triplets().toJavaRDD().distinct().mapToPair(ze -> {
            EdgeTriplet<Long, VisitBean> et = (EdgeTriplet<Long, VisitBean>) ze;
            Long clusterId = et.srcAttr();
            return new Tuple2<Long, Edge<VisitBean>>(clusterId, et);
        });

        ccTriplets.aggregateByKey(new ArrayList<Edge<VisitBean>>(), (u, t) -> {
            u.add(t);
            return u;
        }, (uA, uB) -> {
            ArrayList<Edge<VisitBean>> uC = new ArrayList<>(uA);
            uC.addAll(uB);
            return uC;
        }).flatMap(c -> {
            ArrayList<String> cluster = new ArrayList<>();

            final long id = c._2().size() >= minPts ? bgen.getValue().incrementAndGet() : 0; // check noise

            if (id != 0) {
                System.out.println(String.format("Mapping cluster %d to id %d", c._1(), id));
            }


            for (int i = 0; i < c._2().size(); i++) {
                cluster.add(String.format(Locale.ENGLISH, "%d,%.6f,%.6f,%d",
                        c._2().get(i).srcId(),
                        c._2().get(i).attr().getLat(),
                        c._2().get(i).attr().getLng(),
                        id
                ));
            }

            return cluster;
        }).saveAsTextFile(outputFolder + "/triplets_" + ms + ".csv");

    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public int getMinPts() {
        return minPts;
    }

    public void setMinPts(int minPts) {
        this.minPts = minPts;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }
}
