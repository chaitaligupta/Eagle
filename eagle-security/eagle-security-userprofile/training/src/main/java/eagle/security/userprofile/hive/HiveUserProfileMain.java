package eagle.security.userprofile.hive;

import eagle.job.JobPartitioner;
import eagle.jobhistory.res.JobHistoryContentFilterBuilder;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveUserProfileMain {

    private static final Logger LOG = LoggerFactory.getLogger(HiveUserProfileMain.class);
    private Config hiveConfig;

    public HiveUserProfileMain(){}

    public HiveUserProfileMain(Config config){
        hiveConfig = config;
        LOG.info("config values: " + config.getString("dataSourceConfig.nnEndpoint"));
    }

    public void processHiveQuery(){
        JobHistoryContentFilterBuilder contentFilterBuilder = JobHistoryContentFilterBuilder.newBuilder();

        JobHistoryCrawlConfig.ZKStateConfig zkStateConfig = new JobHistoryCrawlConfig.ZKStateConfig();
        zkStateConfig.zkQuorum = hiveConfig.getString("dataSourceConfig.zkQuorum");
        zkStateConfig.zkRoot = hiveConfig.getString("dataSourceConfig.zkRoot");
        zkStateConfig.zkSessionTimeoutMs = hiveConfig.getInt("dataSourceConfig.zkSessionTimeoutMs");
        zkStateConfig.zkRetryTimes = hiveConfig.getInt("dataSourceConfig.zkRetryTimes");
        zkStateConfig.zkRetryInterval = hiveConfig.getInt("dataSourceConfig.zkRetryInterval");

        LOG.info("zkStateConfig.zkRoot: " + zkStateConfig.zkRoot);

        JobHistoryCrawlConfig.JobHistoryEndpointConfig jobHistoryConfig = new JobHistoryCrawlConfig.JobHistoryEndpointConfig();
        jobHistoryConfig.nnEndpoint = hiveConfig.getString("dataSourceConfig.nnEndpoint");
        jobHistoryConfig.basePath = hiveConfig.getString("dataSourceConfig.basePath");
        jobHistoryConfig.pathContainsJobTrackerName = hiveConfig.getBoolean("dataSourceConfig.pathContainsJobTrackerName");
        jobHistoryConfig.jobTrackerName = hiveConfig.getString("dataSourceConfig.jobTrackerName");
        jobHistoryConfig.zeroBasedMonth = hiveConfig.getBoolean("dataSourceConfig.zeroBasedMonth");


        LOG.info("jobHistoryConfig.pathContainsJobTrackerName: " + jobHistoryConfig.pathContainsJobTrackerName);

        JobHistoryCrawlConfig.ControlConfig controlConfig = new JobHistoryCrawlConfig.ControlConfig();
        controlConfig.dryRun = hiveConfig.getBoolean("dataSourceConfig.dryRun");
        LOG.info("controlConfig.dryRun: " + controlConfig.dryRun);

        try{
            controlConfig.partitionerCls = (Class<? extends JobPartitioner>)Class.forName(hiveConfig.getString("dataSourceConfig.partitionerCls"));
        }catch(Exception ex){
            LOG.error("failing find job id partitioner class " + hiveConfig.getString("dataSourceConfig.partitionerCls"));
            throw new IllegalStateException("jobId partitioner class does not exist " + hiveConfig.getString("dataSourceConfig.partitionerCls"));
        }

        Integer parallelism = 2;
        controlConfig.numTotalPartitions = parallelism;

        UserProfileConfig userProfileConfig = new UserProfileConfig();
        String features = hiveConfig.getString("userProfile.featureSet");
        userProfileConfig.featureSet = features.split(",");
        for(String s: userProfileConfig.featureSet){
            LOG.info("features are: " + s);
        }

        JobHistoryCrawlConfig crawlConfig = new JobHistoryCrawlConfig(zkStateConfig, jobHistoryConfig, controlConfig);
        SparkHiveUserProfileDataCollector sparkHiveCollector = new SparkHiveUserProfileDataCollector(crawlConfig, contentFilterBuilder);
        sparkHiveCollector.crawlJobHistory();
    }

    public static void main(String[] args){
        System.setProperty("config.resource", "./hive-application.conf");
        Config config = ConfigFactory.load();
        HiveUserProfileMain hiveUserProfileMain = new HiveUserProfileMain(config);
        hiveUserProfileMain.processHiveQuery();

    }
}
