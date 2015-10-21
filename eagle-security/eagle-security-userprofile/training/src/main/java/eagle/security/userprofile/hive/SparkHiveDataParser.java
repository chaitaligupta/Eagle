package eagle.security.userprofile.hive;

import eagle.jobhistory.res.JobMessageId;
import eagle.security.hive.ql.HiveQLParserContent;
import eagle.security.hive.ql.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by cgupta on 10/5/15.
 */
public class SparkHiveDataParser {

    private static final Logger LOG = LoggerFactory.getLogger(SparkHiveDataParser.class);

    public Map<String, Object> parse(List<Object> t, JobMessageId messageId) {
        LOG.info("collect called with list size: " + t.size());

        LOG.info("completion time of job: " + messageId.completedTime + ", job id: " + messageId.jobID);

        Map<String, Object> hiveQueryLog = (Map<String, Object>)t.get(1);
        LOG.info("Receive hive query log: " + hiveQueryLog);

        String query = null;
        String db = null;
        String userName = null;
        long timestamp = -1;
        for (Map.Entry<String, Object> entry : hiveQueryLog.entrySet()) {
            switch (entry.getKey()) {
                case "hive.query.string":
                    if (entry.getValue() != null) {
                        query = entry.getValue().toString();
                    }
                    break;
                case "hive.current.database":
                    if (entry.getValue() != null) {
                        db = entry.getValue().toString();
                    }
                    break;
                case "mapreduce.job.user.name":
                    if (entry.getValue() != null) {
                        userName = entry.getValue().toString();
                    }
                    break;
                case "mapreduce.job.cache.files.timestamps":
                    if (entry.getValue() != null) {
                        String timestampString = (String) entry.getValue();
                        String[] timestampArray = timestampString.split("\\s*,\\s*");

                        timestamp = Long.parseLong(timestampArray[0]);
                    }
                    break;
            }
        }

        LOG.info("query: " + query + ", db: " + db + ", username: " + userName + ", timestamp: " + timestamp);

        HiveQLParserContent parserContent;
        Parser queryParser = new Parser();
        try {
            parserContent = queryParser.run(query);
        } catch (Exception ex) {
            LOG.error("Failed running hive query parser.", ex);
            throw new IllegalStateException(ex);
        }
        //
        // Generate "resource" field: /db/table/column
         // "resource" -> </db/table/column1,/db/table/column2,...>
         //

        int noOfCols = 0;
        StringBuilder resources = new StringBuilder();
        String prefix = ",";
        String connector = "/";
        for (Map.Entry<String, Set<String>> entry
                : parserContent.getTableColumnMap().entrySet()) {
            String table = entry.getKey();
            LOG.info("table name: " + table);
            Set<String> colSet = entry.getValue();
            //
             // If colSet is empty, it means no column is accessed in the table.
             // So column is not added to the event stream.
             // Only /db/table
            noOfCols = colSet.size();
            if (colSet.isEmpty()) {
                resources.append(connector).append(db).append(connector).append(table).append(prefix);
            } else {
                for (String col : colSet) {
                    LOG.info("col: " + col);
                    resources.append(connector).append(db).append(connector).append(table);
                    if (col != null && col.length() > 0) {
                        resources.append(connector).append(col);
                    }
                    resources.append(prefix);
                }
            }
        }
        // Remove the last prefix: ","
        resources.setLength(resources.length() - 1);
        LOG.info("resources: " + resources);
        // Format timestamp.
        Date date = new Date(timestamp);
        Format f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = f.format(date);

        // <event> has to be SortedMap.
        Map<String, Object> event = new TreeMap<String, Object>();
        event.put("user", userName);
        event.put("command", parserContent.getOperation());
        event.put("timestamp", time);
        event.put("resource", resources.toString());
        event.put("number_of_resources", String.valueOf(noOfCols));
        LOG.info("HiveQL Parser event stream. " + event);

        return event;

    }
}
