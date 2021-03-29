import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class G32HW1 {

    public static void main(String[] args) throws IOException
    {
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        SparkConf conf = new SparkConf(true).setAppName("ReviewsMNR");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        int K = Integer.parseInt(args[0]), T = Integer.parseInt(args[1]);
        String file = args[2];
        JavaRDD<String> rawdata = sc.textFile(file).repartition(K).cache();

        JavaPairRDD<String, Float> normalizedRatings;

        //map every (ProductID, UserID, Rating, Timestamp) into (userID, (ProductID, Rating))
        normalizedRatings = rawdata.mapToPair((document) -> {
            String[] tokens = document.split(",");
            Tuple2<String, Tuple2<String, Float>> pair = new Tuple2<>(tokens[1], new Tuple2<String, Float>(tokens[0], Float.parseFloat(tokens[2])));
            return pair;
        }).groupByKey().flatMapToPair((document) -> {
        //for each userID return tuples (ProductID, NormalizedRate)
            ArrayList<Tuple2<String, Float>> pairs = new ArrayList<>();
            float sum = 0;
            int num = 0;
            for(Tuple2<String, Float> e : document._2()){
                sum += e._2();
                num++;
            }
            float mean = sum / num;
            for(Tuple2<String, Float> e : document._2())
                pairs.add(new Tuple2<>(e._1(), e._2() - mean));
            return pairs.iterator();
        });

        JavaPairRDD<String, Float> maxNormRatings;

        //For each ProductID return tuple (ProductID, MNR), MNR: max normalized rating for ProductID
        maxNormRatings = normalizedRatings.reduceByKey(Float::max);

        //load the first T elements, ranked from higher MNR to lower MNR, into a list
        List<Tuple2<Float, String>> l = maxNormRatings.mapToPair(Tuple2::swap).sortByKey(false).take(T);

        //print the results
        System.out.println("INPUT PARAMETERS: K=" + K + " T=" + T + " file=" + file + "\n");
        System.out.println("OUTPUT");
        for (Tuple2<Float, String> e : l)
            System.out.println("Product " + e._2() + " maxNormRating " + e._1());
    }
}
