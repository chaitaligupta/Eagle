package eagle.security.hive.job;

import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ControlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.JobHistoryEndpointConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ZKStateConfig;
import eagle.jobhistory.zkres.JobHistoryZKStateManager;
import junit.framework.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TestJobHistoryZKStateManager {
	private static final Logger LOG = LoggerFactory.getLogger(TestJobHistoryZKStateManager.class);
	
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
	public void testWithoutAnything(){
		int numTotalPartitions = 4;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE, -1 * JobHistoryZKStateManager.BACKOFF_DAYS-1); 
		String startingDate = sdf.format(c.getTime());
		JobHistoryZKStateManager state = new JobHistoryZKStateManager(config);
		state.truncateEverything();
		state.ensureJobPartitions(numTotalPartitions);
		for(int i = 0; i < numTotalPartitions; i++){
			String date = state.readProcessedDate(i);
			LOG.info("partition " + i + " has processed date " + date);
			Assert.assertEquals(startingDate, date);
		}
	}
}
