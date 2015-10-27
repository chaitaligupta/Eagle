package eagle.security.hive.job;

import backtype.storm.task.TopologyContext;
import eagle.dataproc.core.ValuesArray;
import eagle.job.DefaultJobPartitionerImpl;
import eagle.jobhistory.res.JobHistoryContentFilter;
import eagle.jobhistory.res.JobHistoryContentFilterBuilder;
import eagle.jobhistory.res.JobHistorySpoutCollectorInterceptor;
import eagle.jobhistory.storm.JobHistorySpout;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ControlConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.JobHistoryEndpointConfig;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig.ZKStateConfig;
import org.junit.Before;

import java.util.regex.Pattern;

public class TestJobHistorySpout {
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
		
		config.controlConfig.partitionerCls = DefaultJobPartitionerImpl.class;
		config.controlConfig.numTotalPartitions = 4;
	}
	
	//@Test
	public void testJobHistorySpout(){
		JobHistoryContentFilter filter = JobHistoryContentFilterBuilder.newBuilder().acceptJobConfFile().
		mustHaveJobConfKeyPatterns(Pattern.compile("hive.query.string")).
		includeJobKeyPatterns(Pattern.compile("hive.query.string"),
				Pattern.compile("mapreduce.job.user.name"))
				.build();
		JobHistorySpoutCollectorInterceptor adaptor = new JobHistorySpoutCollectorInterceptor(){
			@Override
			public void collect(ValuesArray t) {
				System.out.println(t);
			}
		};
		JobHistorySpout spout = new JobHistorySpout(config, filter, adaptor);
		TopologyContext context = new TopologyContext(
				null, null, null, null, null, null, null, null, 0, null, null, null, null, null, null, null); 
		spout.open(null, context, null);
		int i = 10;
		while(i-- > 0){
			spout.nextTuple();
		}
	}
}
