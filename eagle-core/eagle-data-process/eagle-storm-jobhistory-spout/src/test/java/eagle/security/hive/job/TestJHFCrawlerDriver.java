package eagle.security.hive.job;

import eagle.job.DefaultJobPartitionerImpl;
import eagle.job.JobFilter;
import eagle.job.JobFilterByPartition;
import eagle.jobhistory.res.JHFCrawlerDriverImpl;
import eagle.jobhistory.res.JHFInputStreamCallback;
import eagle.jobhistory.res.JobMessageId;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ControlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.JobHistoryEndpointConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ZKStateConfig;
import eagle.jobhistory.zkres.JobHistoryZKStateManager;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class TestJHFCrawlerDriver {
	private final static Logger LOG = LoggerFactory.getLogger(TestJHFCrawlerDriver.class);

	private JobHistoryCrawlConfig config;
	
	@Before
	public void beforeTest(){
		config = new JobHistoryCrawlConfig(new ZKStateConfig(), new JobHistoryEndpointConfig(), 
				new ControlConfig());
		config.jobHistoryConfig.nnEndpoint = "hdfs://sandbox.hortonworks.com:8020";
		config.jobHistoryConfig.basePath = "/mr-history/done";

		config.zkStateConfig.zkQuorum = "127.0.0.1:2181";
		config.zkStateConfig.zkRoot = "/jobhistory";
		config.zkStateConfig.zkSessionTimeoutMs = 1500;
		config.zkStateConfig.zkRetryTimes = 3;
		config.zkStateConfig.zkRetryInterval = 20000;
	}
	
	//@Test
	public void testJHFCrawlerDriver() throws Exception{
		JHFInputStreamCallback callback = new JHFInputStreamCallback() {
			@Override
			public void onInputStream(JobMessageId msgId, InputStream is, Configuration configuration)
					throws Exception {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				String line;
				while((line = reader.readLine()) != null){
					LOG.info(line);
				}
				
				Iterator iter = configuration.iterator();
				while(iter.hasNext()){
					LOG.info(iter.next().toString());
				}
			}
		};
		
		JobFilter jobFilter = new JobFilterByPartition(new DefaultJobPartitionerImpl(), 4, 0);
		
		JobHistoryZKStateManager state = new JobHistoryZKStateManager(config);
		state.ensureJobPartitions(4);
		JHFCrawlerDriverImpl driver = new JHFCrawlerDriverImpl(config.jobHistoryConfig, config.controlConfig, 
				callback, state, jobFilter, 0);
		int i = 5;
		while(i-- > 0){
			driver.crawl();
		}
	}
}
