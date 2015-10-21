package eagle.jobhistory.res;

import java.io.InputStream;
import java.util.List;

/**
 * Define various operations on job history file resource for lifecycle management
 * 
 * The job history file directory structure supported is as follows:
 * <basePath>/<jobTrackerName>/<year>/<month>/<day>/<serialNumber>/<jobHistoryFileName>
 * 
 * In some hadoop version, <jobTrackerName> is not included
 * 
 * The operations involved in resource read
 * - list job tracker names under basePath (mostly basePath is configured in entry mapreduce.jobhistory.done-dir of mapred-site.xml) 
 * - list serial numbers under one day
 * - list job history files under one serial number
 * - read one job history file
 *
 */
public interface JobHistoryLCM {
	public String calculateJobTrackerName(String basePath) throws Exception;
	/**
	 * @param year
	 * @param month 0-based or 1-based month depending on hadoop cluster setting
	 * @param day
	 * @return
	 * @throws Exception
	 */
	public List<String> readSerialNumbers(int year, int month, int day) throws Exception;
	/**
	 * @param year
	 * @param month 0-based or 1-based month depending on hadoop cluster setting
	 * @param day
	 * @param serialNumber
	 * @return
	 * @throws Exception
	 */
	public List<String> readFileNames(int year, int month, int day, int serialNumber) throws Exception;
	/**
	 * @param year
	 * @param month 0-based or 1-based month depending on hadoop cluster setting
	 * @param day
	 * @param serialNumber
	 * @param jobHistoryFileName
	 * @param reader
	 * @throws Exception
	 */
	public void readFileContent(int year, int month, int day, int serialNumber, String jobHistoryFileName, JHFInputStreamCallback reader) throws Exception;
	/**
	 * @param year
	 * @param month 0-based or 1-based month depending on hadoop cluster setting
	 * @param day
	 * @param serialNumber
	 * @param jobHistoryFileName
	 * @return
	 * @throws Exception
	 */
	public InputStream getJHFFileContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception;
	public InputStream getJHFConfContentAsStream(int year, int month, int day, int serialNumber,String jobConfFileName) throws Exception;
}
