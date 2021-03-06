/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eagle.jobrunning.storm;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;

import eagle.dataproc.core.EagleOutputCollector;
import eagle.dataproc.core.ValuesArray;
import eagle.jobrunning.callback.RunningJobMessageId;

public class JobRunningSpoutCollectorInterceptor implements EagleOutputCollector{

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	public void setSpoutOutputCollector(SpoutOutputCollector collector){
		this.collector = collector;
	}

	@Override
	public void collect(ValuesArray t) {
		// the first value is fixed as messageId
		RunningJobMessageId messageId = (RunningJobMessageId) t.get(0);
		List<Object> list = new ArrayList<Object>();
		for (int i = 1; i < t.size(); i++) {
			list.add(t.get(i));
		}
		collector.emit(list, messageId);
	}
}
