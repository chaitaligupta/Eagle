package eagle.jobhistory.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import eagle.job.JobFilter;
import eagle.job.JobFilterByPartition;
import eagle.job.JobPartitioner;
import eagle.jobhistory.res.*;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.zkres.JobHistoryZKStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Zookeeper znode structure
 * -zkRoot
 *   - partitions
 *      - 0 (20150101)
 *      - 1 (20150101)
 *      - 2 (20150101)
 *      - ... ...
 *      - N-1 (20150102)
 *   - jobs
 *      - 20150101
 *        - job1
 *        - job2
 *        - job3
 *      - 20150102
 *        - job1
 *        - job2
 *        - job3
 * 
 * Spout can have multiple instances, which is supported by storm parallelism primitive.
 * 
 * Under znode partitions, N child znodes (name is 0 based integer) would be created with each znode mapped to one spout instance. All jobs will be partitioned into N
 * partitions by applying JobPartitioner class to each job Id. The value of each partition znode is the date when the last job in this partition 
 * is successfully processed.
 * 
 * processing steps
 * 1) In constructor, accept JobHistoryCrawlConfig as input, which includes 
 *    zkStateConfig:zookeeper quorum, zkroot, zk connection timeout, retry times/interval  
 *    JobHistoryEndpointConfig: hdfs endpoint, 
 *    ControlConfig: JobIdPartitoner
 * 2) In open(), calculate jobPartitionId for current spout (which should be exactly same to spout taskId within TopologyContext)
 * 3) In open(), zkState.ensureJobPartitions to rebuild znode partitions if necessary. ensureJobPartitions is only done by one spout task as internally this is using lock
 * 5) In nextTuple(), list job files by invoking hadoop API
 * 6) In nextTuple(), iterate each jobId and invoke JobPartition.partition(jobId) and keep those jobs belonging to current partition Id  
 * 7) process job files (job history file and job configuration xml file)
 * 8) add job Id to current date slot say for example 20150102 after this job is successfully processed
 * 9) clean up all slots with date less than currentProcessDate - 2 days. (2 days should be configurable) 
 *      
 * Note:
 * if one spout instance crashes and is brought up again, open() method would be invoked again, we need think of this scenario.       
 * 
 * @author yonzhang
 *
 */

public class JobHistorySpout extends BaseRichSpout {
	private static final Logger LOG = LoggerFactory.getLogger(JobHistorySpout.class);

	private JobHistoryCrawlConfig config;
	private int partitionId;
	private transient JobHistoryZKStateManager zkState;
	private transient JHFCrawlerDriver driver;
	private JobHistoryContentFilter contentFilter;
	private JobHistorySpoutCollectorInterceptor interceptor;
	private JHFInputStreamCallback callback;
	private ReadWriteLock readWriteLock;
	
	public JobHistorySpout(JobHistoryCrawlConfig config, JobHistoryContentFilter filter){
		this(config, filter, new JobHistorySpoutCollectorInterceptor());
	}
	
	/**
	 * mostly this constructor signature is for unit test purpose as you can put customized interceptor here
	 * @param config
	 * @param filter
	 * @param adaptor
	 */
	public JobHistorySpout(JobHistoryCrawlConfig config, JobHistoryContentFilter filter, JobHistorySpoutCollectorInterceptor adaptor){
		this.config = config;
		this.contentFilter = filter;
		this.interceptor = adaptor;
		this.callback = new DefaultJHFInputStreamCallback(contentFilter, interceptor);
		this.readWriteLock = new ReentrantReadWriteLock();
	}
	
	private int calculatePartitionId(TopologyContext context){
		int thisGlobalTaskId = context.getThisTaskId();
		String componentName = context.getComponentId(thisGlobalTaskId);
		List<Integer> globalTaskIds = context.getComponentTasks(componentName);
		int index = 0;
		for(Integer id : globalTaskIds){
			if(id == thisGlobalTaskId){
				return index;
			}
			index++;
		}
		throw new IllegalStateException();
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			final SpoutOutputCollector collector) {
		partitionId = calculatePartitionId(context);
		// sanity verify 0<=partitionId<=numTotalPartitions-1
		if(partitionId < 0 || partitionId > config.controlConfig.numTotalPartitions){
			throw new IllegalStateException("partitionId should be less than numTotalPartitions with partitionId " + 
					partitionId + " and numTotalPartitions " + config.controlConfig.numTotalPartitions);
		}
		Class<? extends JobPartitioner> partitionerCls = config.controlConfig.partitionerCls;
		JobPartitioner partitioner = null;
		try {
			partitioner = partitionerCls.newInstance();
		} catch (Exception e) {
			LOG.error("failing instantiating job partitioner class " + partitionerCls.getCanonicalName());
			throw new IllegalStateException(e);
		}
		JobFilter jobIdFilter = new JobFilterByPartition(partitioner, config.controlConfig.numTotalPartitions, partitionId);
		zkState = new JobHistoryZKStateManager(config);
		zkState.ensureJobPartitions(config.controlConfig.numTotalPartitions);
		interceptor.setSpoutOutputCollector(collector);
		
		try {
			driver = new JHFCrawlerDriverImpl(config.jobHistoryConfig,
					config.controlConfig,
					callback,
					zkState,
					jobIdFilter,
					partitionId);
		} catch (Exception e) {
			LOG.error("failing creating crawler driver");
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void nextTuple() {
		try{
			driver.crawl();
		}catch(Exception ex){
			LOG.error("fail crawling job history file and continue ...", ex);
            // to avoid frequent exception, sleep a while and try it again
            try{
                Thread.sleep(10*1000);
            }catch(Exception x){
            }
		}
	}

	/**
	 * empty because framework will take care of output fields declaration
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	/**
	 * add to processedJob
	 */
	@Override
    public void ack(Object msgId) {
		JobMessageId messageId = (JobMessageId) msgId;
		LOG.info("ACK on message" + messageId.toString());
		readWriteLock.readLock().lock();
		zkState.addProcessedJob(messageId.completedTime, messageId.jobID);
		readWriteLock.readLock().unlock();
    }

	/**
	 * job is not fully processed
	 */
    @Override
    public void fail(Object msgId) {
		JobMessageId messageId = (JobMessageId) msgId;
		//readWriteLock.readLock().lock();
		//zkState.truncateProcessedJob(messageId.completedTime);
		//readWriteLock.readLock().unlock();
    }
    
    @Override
    public void deactivate() {
    }
    
    @Override
    public void close() {
    }
}
