package eagle.jobhistory.storm.spi;

import eagle.job.JobPartitioner;

import java.io.Serializable;

public class JobHistoryCrawlConfig implements Serializable{
	public ZKStateConfig zkStateConfig;
	public JobHistoryEndpointConfig jobHistoryConfig;
	public ControlConfig controlConfig;

	public JobHistoryCrawlConfig(
			ZKStateConfig zkStateConfig,
			JobHistoryEndpointConfig jobHistoryConfig,
			ControlConfig controlConfig){
		this.zkStateConfig = zkStateConfig;
		this.jobHistoryConfig = jobHistoryConfig;
		this.controlConfig = controlConfig;
	}
	
	public static class ZKStateConfig implements Serializable{
    	public String zkQuorum;
        public String zkRoot;
        public int zkSessionTimeoutMs;
        public int zkRetryTimes;
        public int zkRetryInterval;
    }
    
    public static class JobHistoryEndpointConfig implements Serializable{
    	public String nnEndpoint;
    	public String basePath;
    	public boolean pathContainsJobTrackerName;
    	public String jobTrackerName;
    	public boolean zeroBasedMonth;
    }
    
    public static class ControlConfig implements Serializable{
    	public boolean dryRun;
        public Class<? extends JobPartitioner> partitionerCls;
        public int numTotalPartitions;
    }
}
