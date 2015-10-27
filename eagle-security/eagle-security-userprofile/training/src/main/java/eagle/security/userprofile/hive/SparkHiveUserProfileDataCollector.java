package eagle.security.userprofile.hive;

import eagle.job.JobFilter;
import eagle.job.JobFilterByPartition;
import eagle.job.JobPartitioner;
import eagle.jobhistory.res.*;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.zkres.JobHistoryZKStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by cgupta on 9/28/15.
 */

public class SparkHiveUserProfileDataCollector {
    private static final Logger LOG = LoggerFactory.getLogger(SparkHiveUserProfileDataCollector.class);

    private JobHistoryCrawlConfig jobHistoryCrawlConfig;
    private JobHistoryContentFilterBuilder jobHistoryContentFilter;

    private transient JHFCrawlerDriver driver;
    private JHFInputStreamCallback hiveInputStreamCallback;
    private transient JobHistoryZKStateManager zkState;
    private JobHistoryCollectorInterceptor interceptor;
    private JobHistoryContentFilter contentFilter;
    private SparkHiveDataParser parser;
    private SparkProcessor sparkProcessor;

    public SparkHiveUserProfileDataCollector(){}

    public SparkHiveUserProfileDataCollector(JobHistoryCrawlConfig crawlConfig, JobHistoryContentFilterBuilder contentFilterBuilder){
        jobHistoryCrawlConfig = crawlConfig;
        jobHistoryContentFilter = contentFilterBuilder;
        contentFilter = HiveUserProfileJobHistoryContentFilterBuilder.buildFilter();
        parser = new SparkHiveDataParser();
        sparkProcessor = new SparkProcessor();
        interceptor = new JobHistoryCollectorInterceptor(parser, sparkProcessor);
        this.hiveInputStreamCallback = new DefaultJHFInputStreamCallback(contentFilter, interceptor);
        zkState = new JobHistoryZKStateManager(crawlConfig);
        zkState.ensureJobPartitions(jobHistoryCrawlConfig.controlConfig.numTotalPartitions);
    }


    public void crawlJobHistory(){

        JobPartitioner partitioner = null;
        try {
            partitioner = jobHistoryCrawlConfig.controlConfig.partitionerCls.newInstance();
        } catch (Exception e) {
            LOG.error("failing instantiating job partitioner class " + jobHistoryCrawlConfig.controlConfig.partitionerCls.getCanonicalName());
            throw new IllegalStateException(e);
        }

        JobFilter jobIdFilter = new JobFilterByPartition(partitioner, jobHistoryCrawlConfig.controlConfig.numTotalPartitions, 0);
        try{
            driver = new JHFCrawlerDriverImpl(jobHistoryCrawlConfig.jobHistoryConfig,
                    jobHistoryCrawlConfig.controlConfig, hiveInputStreamCallback,
                    zkState, jobIdFilter, 1);
            driver.crawl();

            /*JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            JavaRDD<Integer> rdd = javaSparkContext.parallelize();
            */

        }catch (Exception e){
            LOG.error("could not create the Job History Crawler....");
            e.printStackTrace();
            try{
                Thread.sleep(10*1000);
            }catch(Exception x){
                LOG.error("thread sleep throw exception");
            }
        }
    }

    public List<String> getHiveQeueryResource(){
        return new LinkedList<String>();
    }
}
