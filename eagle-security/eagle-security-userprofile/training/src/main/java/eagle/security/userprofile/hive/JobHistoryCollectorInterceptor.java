package eagle.security.userprofile.hive;

import eagle.dataproc.core.EagleOutputCollector;
import eagle.dataproc.core.ValuesArray;
import eagle.jobhistory.res.JobMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cgupta on 9/28/15.
 */
public class JobHistoryCollectorInterceptor implements EagleOutputCollector {

    private static final Logger LOG = LoggerFactory.getLogger(JobHistoryCollectorInterceptor.class);
    private SparkHiveDataParser parser;
    private SparkProcessor processor;

    public JobHistoryCollectorInterceptor(SparkHiveDataParser parser, SparkProcessor processor){
        this.parser = parser;
        this.processor = processor;
    }
    @Override
    public void collect(ValuesArray t) {
        JobMessageId messageId = (JobMessageId) t.get(0);
        LOG.info("messageId: " + messageId);
        List<Object> list = new ArrayList<Object>();
        for (int i = 1; i < t.size(); i++) {
            list.add(t.get(i));
        }

        processor.processData(parser.parse(list, messageId));

    }
}
