package eagle.jobhistory.res;

import backtype.storm.spout.SpoutOutputCollector;

import eagle.dataproc.core.EagleOutputCollector;
import eagle.dataproc.core.ValuesArray;

import java.util.ArrayList;
import java.util.List;

public class JobHistorySpoutCollectorInterceptor implements EagleOutputCollector{
	private SpoutOutputCollector collector;
	
	public void setSpoutOutputCollector(SpoutOutputCollector collector){
		this.collector = collector;
	}

	@Override
	public void collect(ValuesArray t) {
		// the first value is fixed as messageId
		JobMessageId messageId = (JobMessageId) t.get(0);
		List<Object> list = new ArrayList<Object>();
		for (int i = 1; i < t.size(); i++) {
			list.add(t.get(i));
		}
		collector.emit(list, messageId);
	}
}
