package eagle.security.hive.jobhistory;

import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.base.BaseRichSpout;

import eagle.job.JobPartitioner;
import eagle.jobhistory.res.JobHistoryContentFilter;
import eagle.jobhistory.storm.JobHistorySpout;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ControlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.JobHistoryEndpointConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ZKStateConfig;

/**
 * create spout for job history data collection
 * @author yonzhang
 */

public class HiveJobHistorySourcedStormSpoutProvider {
	private static final Logger LOG = LoggerFactory.getLogger(HiveJobHistorySourcedStormSpoutProvider.class);

	public BaseRichSpout getSpout(Config config, int parallelism) {
		JobHistoryContentFilter filter = HiveJobHistoryContentFilterBuilder.buildFilter();
		
		ZKStateConfig zkStateConfig = new ZKStateConfig();
		zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
		zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");
		zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
		zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
		zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");
		
		JobHistoryEndpointConfig jobHistoryConfig = new JobHistoryEndpointConfig();
		jobHistoryConfig.nnEndpoint = config.getString("dataSourceConfig.nnEndpoint");
		jobHistoryConfig.basePath = config.getString("dataSourceConfig.basePath");
		jobHistoryConfig.pathContainsJobTrackerName = config.getBoolean("dataSourceConfig.pathContainsJobTrackerName");
		jobHistoryConfig.jobTrackerName = config.getString("dataSourceConfig.jobTrackerName");
		jobHistoryConfig.zeroBasedMonth = config.getBoolean("dataSourceConfig.zeroBasedMonth");
		
		ControlConfig controlConfig = new ControlConfig();
		controlConfig.dryRun = config.getBoolean("dataSourceConfig.dryRun");
		controlConfig.numTotalPartitions = parallelism <= 0 ? 1 : parallelism;
		try{
			controlConfig.partitionerCls = (Class<? extends JobPartitioner>)Class.forName(config.getString("dataSourceConfig.partitionerCls"));
		}catch(Exception ex){
			LOG.error("failing find job id partitioner class " + config.getString("dataSourceConfig.partitionerCls"));
			throw new IllegalStateException("jobId partitioner class does not exsit " + config.getString("dataSourceConfig.partitionerCls"));
		}
		
		JobHistoryCrawlConfig crawlConfig = new JobHistoryCrawlConfig(zkStateConfig, jobHistoryConfig, controlConfig);
		JobHistorySpout spout = new JobHistorySpout(crawlConfig, filter);
		return spout;
	}
}
