package eagle.jobhistory.res;

import eagle.job.JobFilter;
import eagle.jobhistory.storm.spi.JobHistoryCrawlConfig;
import eagle.jobhistory.zkres.JobHistoryZKStateLCM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * single thread crawling per driver
 * multiple drivers can achieve parallelism 
 *
 */
public class JHFCrawlerDriverImpl implements JHFCrawlerDriver{
	private static final Logger LOG = LoggerFactory.getLogger(JHFCrawlerDriverImpl.class);

	private final static int SLEEP_SECONDS_WHILE_QUEUE_IS_EMPTY = 10;
	private final static String FORMAT_JOB_PROCESS_DATE = "%4d%02d%02d";
	private final static Pattern PATTERN_JOB_PROCESS_DATE = Pattern.compile("([0-9]{4})([0-9]{2})([0-9]{2})");
	private int m_flag = TODAY; // 0 not set, 1 TODAY, 2 BEFORETODAY
	private static final int INITIALIZED = 0x0;
	private static final int TODAY = 0x1;
	private static final int BEFORETODAY = 0x10;

	private Deque<String> processQueue = new LinkedList<String>();
	private Set<String> processedJobFileNames = new HashSet<String>();
	private final int PROCESSED_JOB_KEEP_DAYS = 2;

	private final JobProcessDate status = new JobProcessDate();
	private boolean dryRun;
	private JHFInputStreamCallback reader;
	protected boolean m_zeroBasedMonth = true;
	
	private JobHistoryZKStateLCM zkStatelcm;
	private JobHistoryLCM jhfLCM;
	private JobFilter jobFilter;
	private int partitionId;
	private TimeZone utcTZ = TimeZone.getTimeZone("UTC");
	
	public JHFCrawlerDriverImpl(JobHistoryCrawlConfig.JobHistoryEndpointConfig jobHistoryConfig,
			JobHistoryCrawlConfig.ControlConfig controlConfig, JHFInputStreamCallback reader, 
			JobHistoryZKStateLCM zkStateLCM, JobFilter jobFilter, int partitionId) throws Exception {
		this.m_zeroBasedMonth = jobHistoryConfig.zeroBasedMonth;
		this.dryRun = controlConfig.dryRun;
		if(this.dryRun)  LOG.info("this is a dry run");
		this.reader = reader;
		jhfLCM = new JobHistoryDAOImpl(jobHistoryConfig.nnEndpoint, jobHistoryConfig.basePath, 
				jobHistoryConfig.pathContainsJobTrackerName, jobHistoryConfig.jobTrackerName);
		this.zkStatelcm = zkStateLCM;
		this.partitionId = partitionId;
		this.jobFilter = jobFilter;
	}
	
	/**
	 * <br>
	 * 1. if queue is not empty <br>
	 * 1.1 dequeue and process one job file <br>
	 * 1.2 store processed job file and also cache it to processedJobFileNames
	 * 2. if queue is empty <br>
	 * 2.0 if flag is BEFORETODAY, then write currentProcessedDate to jobProcessedDate as this day's data are all processed <br>
	 * 2.1 crawl that day's job file list <br>
	 * 2.2 filter out those jobID which are in _processedJobIDs keyed by
	 * currentProcessedDate <br>
	 * 2.3 put available file list to processQueue and then go to step 1
	 */
	@Override
	public void crawl() throws Exception {
		LOG.info("Hive history job processing queue size is " + processQueue.size());
		while (processQueue.isEmpty()) {
			if (m_flag == INITIALIZED) {
				readAndCacheLastProcessedDate();
			}
			if (m_flag == BEFORETODAY) {
				updateProcessDate();
				clearProcessedJobFileNames();
			}
			if (m_flag != TODAY) { // advance one day if initialized or BEFORE today
				advanceOneDay();
			}

			if (isToday()) {
				m_flag = TODAY;
			} else {
				m_flag = BEFORETODAY;
			}
			List<String> serialNumbers = jhfLCM.readSerialNumbers(this.status.year, getActualMonth(status.month), this.status.day);
			for(String serialNumber : serialNumbers){
				List<String> jobHistoryFiles = 
						jhfLCM.readFileNames(this.status.year, getActualMonth(status.month), this.status.day,Integer.parseInt(serialNumber));
				LOG.info("total number of job history files " + jobHistoryFiles.size());
				for(String jobHistoryFile : jobHistoryFiles){
					if (jobFilter.accept(jobHistoryFile) && !fileProcessed(jobHistoryFile)) {
						processQueue.add(jobHistoryFile);
					}
				}
				LOG.info("after filtering, number of job history files " + processQueue.size());
			}
			if (processQueue.isEmpty()) {
				Thread.sleep(SLEEP_SECONDS_WHILE_QUEUE_IS_EMPTY * 1000);
			} else {
				LOG.info("queue size after populating is now : " + processQueue.size());
			}
		}
		// start to process job history file
		// change to read all the files??
		while(!processQueue.isEmpty()) {
			String jobHistoryFile = processQueue.pollFirst();
			if (jobHistoryFile == null) { // terminate this round of crawling when the queue is empty
				LOG.info("process queue is empty, ignore this round");
				return;
			}
			// get serialNumber from job history file name
			Pattern p = Pattern.compile("^job_[0-9]+_([0-9]+)[0-9]{3}[_-]{1}");
			Matcher m = p.matcher(jobHistoryFile);
			String serialNumber = null;

			if (m.find()) {
				serialNumber = m.group(1);
			} else {
				LOG.warn("illegal job history file name : " + jobHistoryFile);
				return;
			}
			if (!dryRun) {
				jhfLCM.readFileContent(status.year, getActualMonth(status.month), status.day, Integer.valueOf(serialNumber), jobHistoryFile, reader);
			}
			//zkStatelcm.addProcessedJob(String.format(FORMAT_JOB_PROCESS_DATE, this.status.year, this.status.month + 1, this.status.day), jobHistoryFile);
			processedJobFileNames.add(jobHistoryFile);
			LOG.info("processedJobFileNames size: " + processedJobFileNames.size());
		}
		return;
	}

	private void updateProcessDate() throws Exception {
		String line = String.format(FORMAT_JOB_PROCESS_DATE, this.status.year,
				getActualMonth(this.status.month), this.status.day);
		zkStatelcm.updateProcessedDate(partitionId, line);
	}

	private int getActualMonth(int month){
		return m_zeroBasedMonth ? status.month : status.month+1;
	}
	
	private static class JobProcessDate {
		public int year;
		public int month; // 0 based month
		public int day;
	}

	private void clearProcessedJobFileNames() {
		processedJobFileNames.clear();
	}

	private void readAndCacheLastProcessedDate() throws Exception {
		String lastProcessedDate = zkStatelcm.readProcessedDate(partitionId);
		LOG.info("lastProcessedData: " + lastProcessedDate);
		Matcher m = PATTERN_JOB_PROCESS_DATE.matcher(lastProcessedDate);
		if (m.find() && m.groupCount() == 3) {
			this.status.year = Integer.parseInt(m.group(1));
			this.status.month = Integer.parseInt(m.group(2)) - 1; // zero based month
			this.status.day = Integer.parseInt(m.group(3));
		} else {
			throw new IllegalStateException("job lastProcessedDate must have format YYYYMMDD " + lastProcessedDate);
		}
		
		GregorianCalendar cal = new GregorianCalendar(utcTZ);
		cal.set(this.status.year, this.status.month, this.status.day, 0, 0, 0);
		cal.add(Calendar.DATE, 1);
		List<String> list = zkStatelcm.readProcessedJobs(String.format(FORMAT_JOB_PROCESS_DATE, cal.get(Calendar.YEAR),
				getActualMonth(cal.get(Calendar.MONTH)), cal.get(Calendar.DAY_OF_MONTH)));
		if(list != null)
			this.processedJobFileNames = new HashSet<String>(list);  
	}

	private void advanceOneDay() throws Exception {
		GregorianCalendar cal = new GregorianCalendar(utcTZ);
		cal.set(this.status.year, this.status.month, this.status.day, 0, 0, 0);
		cal.add(Calendar.DATE, 1);
		this.status.year = cal.get(Calendar.YEAR);
		this.status.month = cal.get(Calendar.MONTH);
		this.status.day = cal.get(Calendar.DAY_OF_MONTH);
		
//		clearProcessedJob(cal);
	}

	private void clearProcessedJob(Calendar cal){
		// clear all already processed jobs some days before current processing date (PROCESSED_JOB_KEEP_DAYS)
		cal.add(Calendar.DATE, -1-PROCESSED_JOB_KEEP_DAYS);
		String line = String.format(FORMAT_JOB_PROCESS_DATE, cal.get(Calendar.YEAR),
				getActualMonth(cal.get(Calendar.MONTH)), cal.get(Calendar.DAY_OF_MONTH));
		zkStatelcm.truncateProcessedJob(line);
	}
	
	private boolean isToday() {
		GregorianCalendar today = new GregorianCalendar(utcTZ);

		if (today.get(Calendar.YEAR) == this.status.year
				&& today.get(Calendar.MONTH) == this.status.month
				&& today.get(Calendar.DAY_OF_MONTH) == this.status.day)
			return true;

		return false;
	}

	/**
	 * check if this file was already processed
	 * 
	 * @param fileName
	 * @return
	 */
	private boolean fileProcessed(String fileName) {
		if (processedJobFileNames.contains(fileName))
			return true;
		return false;
	}
}
