package eagle.jobhistory.res;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class JobHistoryDAOImpl extends AbstractJobHistoryDAO{
	private static final Logger LOG = LoggerFactory.getLogger(JobHistoryDAOImpl.class);
	
	private Configuration m_conf = new Configuration();
	
	public JobHistoryDAOImpl(String hdfsEndpoint, String basePath, boolean pathContainsJobTrackerName, String jobTrackerName) throws Exception{
		super(basePath, pathContainsJobTrackerName, jobTrackerName);
		this.m_conf.set("fs.defaultFS", hdfsEndpoint);
		this.m_conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		this.m_conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.m_conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		LOG.info("file system:" + hdfsEndpoint);
	}
	
	@Override
	public String calculateJobTrackerName(String basePath) throws Exception{
		String latestJobTrackerName = null;
		FileSystem hdfs = null;
		try{
			hdfs = FileSystem.get(m_conf);
			Path hdfsFile = new Path(basePath);
			FileStatus[] files = hdfs.listStatus(hdfsFile);

			// Sort by modification time as order of desc
			Arrays.sort(files, new Comparator<FileStatus>() {
				@Override
				public int compare(FileStatus o1, FileStatus o2) {
					long comp = parseJobTrackerNameTimestamp(o1.getPath().toString()) - parseJobTrackerNameTimestamp(o2.getPath().toString());
					if (comp > 0l) {
						return -1;
					} else if (comp < 0l) {
						return 1;
					}
					return 0;
				}
			});

			for(FileStatus fs : files){
				// back-compatible with hadoop 0.20
				// pick the first directory file which should be the latest modified.
				if(fs.isDir()){
					latestJobTrackerName = fs.getPath().getName();
					break;
				}
			}
		}catch(Exception ex){
			LOG.error("fail read job tracker name " + basePath, ex);
			throw ex;
		}finally{
			if(hdfs != null)
				hdfs.close();
		}
		return latestJobTrackerName == null ? "" : latestJobTrackerName;
	}
	
	@Override
	public List<String> readSerialNumbers(int year, int month, int day) throws Exception{
		List<String> serialNumbers = new ArrayList<String>();
		FileSystem hdfs = null;
		String dailyPath = buildWholePathToYearMonthDay(year, month, day);
		LOG.info("crawl serial numbers under one day : " + dailyPath);
		try{
			hdfs = FileSystem.get(m_conf);
			Path hdfsFile = new Path(dailyPath);
			FileStatus[] files = hdfs.listStatus(hdfsFile);
			for(FileStatus fs : files){
				if(fs.isDir()){
					serialNumbers.add(fs.getPath().getName());
				}
			}
		}catch(java.io.FileNotFoundException ex){
			LOG.warn("continue to crawl with failure to find file " + dailyPath);
			LOG.debug("continue to crawl with failure to find file " + dailyPath, ex);
			// continue to execute
			return serialNumbers;
		}catch(Exception ex){
			LOG.error("fail reading serial numbers under one day " + dailyPath, ex);
			throw ex;
		}finally{
			if(hdfs != null)
				hdfs.close();
		}
		StringBuilder sb = new StringBuilder();
		for(String sn : serialNumbers){
			sb.append(sn);sb.append(",");
		}
		LOG.info("crawled serialNumbers: " + sb);
		return serialNumbers;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public List<String> readFileNames(int year, int month, int day, int serialNumber) throws Exception{
		LOG.info("crawl file names under one serial number : " + year + "/" + month + "/" + day + ":" + serialNumber);
		List<String> jobFileNames = new ArrayList<String>();
		FileSystem hdfs = null;
		String serialPath = buildWholePathToSerialNumber(year, month, day, serialNumber);
		LOG.info("serial Path: " + serialPath);
		try{
			hdfs = FileSystem.get(m_conf);
			Path hdfsFile = new Path(serialPath);
			// filter those files which is job configuration file in xml format
			FileStatus[] files = hdfs.listStatus(hdfsFile, new PathFilter(){
				@Override
				public boolean accept(Path path){
					if(path.getName().endsWith(".xml"))
						return true;
					return false;
				}
			});
			LOG.info("serial path found, number of files: " + files.length);
			for(FileStatus fs : files){
				if(!fs.isDir()){
					jobFileNames.add(fs.getPath().getName());
				}
			}
			if(LOG.isDebugEnabled()){
				StringBuilder sb = new StringBuilder();
				for(String sn : jobFileNames){
					sb.append(sn);sb.append(",");
				}
				LOG.debug("crawled: " + sb);
			}
		}catch(Exception ex){
			LOG.error("fail reading job history file names under serial number " + serialPath, ex);
			throw ex;
		}finally{
			if(hdfs != null)
				hdfs.close();
		}
		return jobFileNames;
	}
	
	/**
	 * it's the responsibility of caller to close input stream
	 */
	@Override
	public InputStream getJHFFileContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception{
		FileSystem hdfs = null;
		String path = buildWholePathToJobHistoryFile(year, month, day, serialNumber, jobHistoryFileName);
		LOG.info("Read job history file: " + path);
		try{
			hdfs = FileSystem.get(m_conf);
			Path hdfsFile = new Path(path);
			return hdfs.open(hdfsFile);
		}catch(Exception ex){
			LOG.error("fail getting hdfs file inputstream " + path, ex);
			throw ex;
		}
	}

	/**
	 * it's the responsibility of caller to close input stream
	 */
	@Override
	public InputStream getJHFConfContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception {
		String path = buildWholePathToJobConfFile(year, month, day, serialNumber,jobHistoryFileName);
		if(path  == null) return null;

		LOG.info("Read job conf file: " + path);
		try{
			FileSystem hdfs  = FileSystem.get(m_conf);
			Path hdfsFile = new Path(path);
			return hdfs.open(hdfsFile);
		}catch(Exception ex){
			LOG.error("fail getting job configuration input stream from " + path, ex);
			throw ex;
		}
	}
}
