package eagle.security.userprofile.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cgupta on 10/6/15.
 */

class FeatureGenerator implements Function<Map<String, Object>, Map<String, Feature>> {

    private static final Logger LOGFILTER = LoggerFactory.getLogger(FeatureGenerator.class);
    @Override
    public Map<String, Feature> call(Map<String, Object> v1) throws Exception {
        LOGFILTER.info("Inside map call");
        Feature aFeature = new Feature();
        Map<String, Feature> returnMap = new HashMap<>();
        for(String key:v1.keySet()){
            aFeature.command = (String)((key.equalsIgnoreCase("command"))?v1.get(key):aFeature.command);
            aFeature.noOfResources = (String)((key.equalsIgnoreCase("number_of_resources"))?v1.get(key):aFeature.noOfResources);
            aFeature.resource = (String)((key.equalsIgnoreCase("resource"))?v1.get(key):aFeature.resource);
            aFeature.user = (String)((key.equalsIgnoreCase("user"))?v1.get(key):aFeature.user);
            aFeature.timestamp = (String)((key.equalsIgnoreCase("timestamp"))?v1.get(key):aFeature.timestamp);
        }
        returnMap.put(aFeature.user, aFeature);
        return returnMap;
    }
}

class Feature{
    public String user;
    public String command;
    public String noOfResources;
    public String resource;
    public String timestamp;
}

class FilterGenerator implements Function<Map<String, Feature>, Boolean> {
    private static final Logger LOGFILTER = LoggerFactory.getLogger(FilterGenerator.class);

    @Override
    public Boolean call(Map<String, Feature> v1) throws Exception {
        LOGFILTER.info("Inside filter call");
        return ((v1.get("b_ebayadvertising") == null)? false:true);
    }
}

class TestMap implements Function<Integer, Integer>{
    @Override
    public Integer call(Integer x) { return x*x; }

}

public class SparkProcessor implements Serializable{

    private static final Logger LOG = LoggerFactory.getLogger(SparkProcessor.class);

    private SparkConf sparkConf;
    private JavaSparkContext javaSparkContext;

    public SparkProcessor(){
        sparkConf = new SparkConf().setAppName("hive userprofile");
        sparkConf.setMaster("local");
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    public void processData(Map<String, Object> events){

        LOG.info("process data called");
        List<Tuple2<String, String>> scalaEventsList = new ArrayList<>();

        List<Map<String, Object>> mapToList = new ArrayList<>();
        mapToList.add(events);

        JavaRDD<Map<String, Object>> rddMap = javaSparkContext.parallelize(mapToList);
        FeatureGenerator featureGenerator = new FeatureGenerator();
        JavaRDD<Map<String, Feature>> anotherFeatureRDD = rddMap.map(featureGenerator);
        LOG.info("********* anotherFeatureRDD: " + anotherFeatureRDD.count());
        FilterGenerator filterGenerator = new FilterGenerator();
        JavaRDD<Map<String, Feature>> filteredFeatureRDD = anotherFeatureRDD.filter(filterGenerator);
        LOG.info("********* filteredFeatureRDD: " + filteredFeatureRDD.count());
        //filteredFeatureRDD.reduce()

    }
}
