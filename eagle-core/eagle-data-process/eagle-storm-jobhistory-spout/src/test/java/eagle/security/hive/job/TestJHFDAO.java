package eagle.security.hive.job;

import eagle.jobhistory.res.JHFInputStreamCallback;
import eagle.jobhistory.res.JobHistoryDAOImpl;
import eagle.jobhistory.res.JobMessageId;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

public class TestJHFDAO {
	private final static Logger LOG = LoggerFactory.getLogger(TestJHFDAO.class);
	
	//@Test
	public void testJHFCrawler() throws Exception{
		JobHistoryDAOImpl dao = new JobHistoryDAOImpl("hdfs://sandbox.hortonworks.com:8020", "/mr-history/done", false, null);
		List<String> series = dao.readSerialNumbers(2015, 06, 27);
		LOG.info(series.toString());
		List<String> files = dao.readFileNames(2015, 06, 27, 0);
		LOG.info(files.toString());
		
		JHFInputStreamCallback callback = new JHFInputStreamCallback() {
			@Override
			public void onInputStream(JobMessageId msgId, InputStream is, Configuration configuration)
					throws Exception {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				String line;
				while((line = reader.readLine()) != null){
					LOG.info(line);
				}
				
				Iterator iter = configuration.iterator();
				while(iter.hasNext()){
					LOG.info(iter.next().toString());
				}
			}
		};
		dao.readFileContent(2015, 06, 27, 0, "job_1435084528164_0010-1435372154197-hdfs-select+*+from+custome...umber%3Db.phone_number%28Stage-1435372198149-1-0-SUCCEEDED-default-1435372166475.jhist", callback);
	}
}
