package eagle.jobhistory.res;

import java.io.InputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

/**
 * callback when job history file input stream is ready
 */
public interface JHFInputStreamCallback extends Serializable{
	/**
	 * this is called when job file string and job configuration file is ready
	 * @param is
	 * @param configuration
	 * @throws Exception
	 */
	public void onInputStream(JobMessageId msgId, InputStream is, Configuration configuration) throws Exception;
}
