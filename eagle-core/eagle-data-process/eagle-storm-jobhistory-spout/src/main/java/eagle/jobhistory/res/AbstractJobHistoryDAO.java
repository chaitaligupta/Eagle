package eagle.jobhistory.res;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * job history is the resource
 */
public abstract class AbstractJobHistoryDAO implements JobHistoryLCM {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractJobHistoryDAO.class);
	
	private final static String yearUrlFormat = "/%4d";
	private final static String monthUrlFormat = "/%02d";
	private final static String dayUrlFormat = "/%02d";
	private final static String yearMonthDayUrlFormat = yearUrlFormat + monthUrlFormat + dayUrlFormat;
	protected final static String serialUrlFormat = "/%06d";
	protected final static String fileUrlFormat = "/%s";
	private static final Pattern JOBTRACKERNAME_PATTERN = Pattern.compile("^.*_(\\d+)_$");;
	protected static final Pattern JOBID_PATTERN = Pattern.compile("job_\\d+_\\d+");
	private final static String FORMAT_JOB_PROCESS_DATE = "%4d%02d%02d";
	
	protected final String m_CHAR_SET = "ISO-8859-1";

	protected final String m_basePath;
	protected volatile String m_jobTrackerName;
	
	public static final String CONFIG_SEPERATOR = ".";
	public  static final String JOB_CONF_POSTFIX="_conf.xml";
	
	private final static Timer timer = new Timer(true);
	private final static long jobTrackerSyncDuration = 10 * 60 * 1000; // 10 minutes
	
	private boolean m_pathContainsJobTrackerName;

	public AbstractJobHistoryDAO(String basePath, boolean pathContainsJobTrackerName, String startingJobTrackerName) throws Exception{
		m_basePath = basePath;
		m_pathContainsJobTrackerName = pathContainsJobTrackerName;
		m_jobTrackerName = startingJobTrackerName;
		if(m_pathContainsJobTrackerName){
			if(startingJobTrackerName == null || startingJobTrackerName.isEmpty())
				throw new IllegalStateException("startingJobTrackerName should not be null or empty");
			// start background thread to check what is current job tracker 
			startThread(m_basePath);
		}
	}
	
	protected String buildWholePathToYearMonthDay(int year, int month, int day){
		StringBuilder sb = new StringBuilder();
		sb.append(m_basePath);
		if(!m_pathContainsJobTrackerName && m_jobTrackerName != null && !m_jobTrackerName.isEmpty()){
			sb.append("/");
			sb.append(m_jobTrackerName);
		}
		sb.append(String.format(yearMonthDayUrlFormat, year, month, day));
		return sb.toString();
	}

	protected String buildWholePathToSerialNumber(int year, int month, int day, int serialNumber){
		LOG.info("serialNumber: " + serialNumber);
		String wholePathToYearMonthDay = buildWholePathToYearMonthDay(year, month, day);
		StringBuilder sb = new StringBuilder();
		sb.append(wholePathToYearMonthDay);
		sb.append(String.format(serialUrlFormat, serialNumber));
		return sb.toString();
	}

	protected String buildWholePathToJobHistoryFile(int year, int month, int day, int serialNumber, String jobHistoryFileName){
		String wholePathToJobHistoryFile = buildWholePathToSerialNumber(year, month, day, serialNumber);
		LOG.info("wholePathToJobHistoryFile: " + wholePathToJobHistoryFile);
		StringBuilder sb = new StringBuilder();
		sb.append(wholePathToJobHistoryFile);
		sb.append(String.format(fileUrlFormat, jobHistoryFileName));
		return sb.toString();
	}


	protected String buildWholePathToJobConfFile(int year, int month, int day, int serialNumber,String jobHistFileName){
		Matcher matcher = JOBID_PATTERN.matcher(jobHistFileName);
		if(matcher.find()){
			String wholePathToJobConfFile = buildWholePathToSerialNumber(year, month, day, serialNumber);
			StringBuilder sb = new StringBuilder();
			sb.append(wholePathToJobConfFile);
			sb.append("/");
			sb.append(String.format(fileUrlFormat, matcher.group()));
			sb.append(JOB_CONF_POSTFIX);
			return sb.toString();
		}
		LOG.warn("Illegal job history file name: "+jobHistFileName);
		return null;
	}

	private void startThread(final String basePath) throws Exception{
		LOG.info("start an every-"+jobTrackerSyncDuration / (60 * 1000)+"min timer task to check current jobTrackerName in background");
		// Automatically update current job tracker name in background every 30 minutes
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
					LOG.info("regularly checking current jobTrackerName in background");
					final String _jobTrackerName = calculateJobTrackerName(basePath);
					if(_jobTrackerName!=null && !_jobTrackerName.equals(m_jobTrackerName)){
						LOG.info("jobTrackerName changed from "+m_jobTrackerName+" to "+_jobTrackerName);
						m_jobTrackerName = _jobTrackerName;
					}
					LOG.info("Current jobTrackerName is: "+m_jobTrackerName);
				} catch (Exception e) {
					LOG.error("failed to figure out current job tracker name that is not configured due to: "+e.getMessage(),e);
				} catch (Throwable t){
					LOG.error("failed to figure out current job tracker name that is not configured due to: "+t.getMessage(),t);
				}
			}
		},jobTrackerSyncDuration,jobTrackerSyncDuration);
	}


	@Override
	public void readFileContent(int year, int month, int day, int serialNumber, String jobHistoryFileName, JHFInputStreamCallback reader) throws Exception{
		InputStream downloadIs = null;
		try{
			downloadIs = getJHFFileContentAsStream(year, month, day, serialNumber, jobHistoryFileName);
		}catch (FileNotFoundException ex){
			LOG.error("job history file not found " + jobHistoryFileName+", ignore and will NOT process any more");
			return;
		}
				
		InputStream downloadJobConfIs = null;
		try{
			downloadJobConfIs = getJHFConfContentAsStream(year, month, day, serialNumber,jobHistoryFileName);
		}catch(FileNotFoundException ex){
			LOG.warn("job configuration file of "+ jobHistoryFileName+" not found , ignore and use empty configuration");
		}

		org.apache.hadoop.conf.Configuration  conf = null;

		if(downloadJobConfIs!=null){
			conf = new org.apache.hadoop.conf.Configuration();
			conf.addResource(downloadJobConfIs);
		}

		//String jobId = null;
		//Matcher matcher = JOBID_PATTERN.matcher(jobHistoryFileName);
		//if(matcher.find()){
		//	jobId = matcher.group();
		//}

		try {
			if (downloadIs != null) {
				reader.onInputStream(new JobMessageId(jobHistoryFileName, String.format(FORMAT_JOB_PROCESS_DATE, year, month, day)), downloadIs, conf);
			}
		} catch (Exception ex) {
			LOG.error("fail reading job history file", ex);
			throw ex;
		}catch(Throwable t){
			LOG.error("fail reading job history file", t);
			throw new Exception(t);
		}finally {
			try{
				if(downloadJobConfIs != null){
					downloadJobConfIs.close();
				}
				if(downloadIs != null){
					downloadIs.close();
				}
			}catch (IOException e){
				LOG.error(e.getMessage(),e);
			}
		}
	}
	

	protected static long parseJobTrackerNameTimestamp(String jtname){
		Matcher matcher = JOBTRACKERNAME_PATTERN.matcher(jtname);
		if(matcher.find()){
			return Long.parseLong(matcher.group(1));
		}
		LOG.warn("invalid job tracker name: "+jtname);
		return -1;
	}
}
