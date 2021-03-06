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
package eagle.security.userprofile;

import kafka.utils.VerifiableProperties;
import kafka.utils.Utils;

public class UserPartitioner implements kafka.producer.Partitioner{
	public UserPartitioner(VerifiableProperties prop){
		
	}

	@Override
	public int partition(Object arg0, int arg1) {
		String user = (String)arg0;
		return  Utils.abs(user.hashCode()) % arg1;
	}
}