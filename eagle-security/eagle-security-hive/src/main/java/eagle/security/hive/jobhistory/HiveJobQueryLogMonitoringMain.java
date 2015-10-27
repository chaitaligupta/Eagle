package eagle.security.hive.jobhistory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import eagle.datastream.ExecutionEnvironmentFactory;
import eagle.datastream.StormExecutionEnvironment;
import eagle.security.hive.sensitivity.HiveResourceSensitivityDataJoinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJobQueryLogMonitoringMain {
	private static final Logger LOG = LoggerFactory.getLogger(HiveJobQueryLogMonitoringMain.class);
	
	/*public static void main(String[] args) throws Exception{
		System.setProperty("config.resource", "/data-process-runtime-hive-jobhistory.conf");
		LOG.info("start hive query log monitoring program ...");
		HighLevelExecutor executor = new HighLevelExecutor("hadoop.jobhistory.hive.query.log.monitoring") {
			private List<Task> hiveQueryAlertTasks = new ArrayList<Task>();
			@Override
			protected AbstractTopologyCompiler getTopologyCompiler(
					Config config, AbstractTaskGraph graph) {
				return new StormTopologyCompiler(config, graph);
			}

			@Override
			protected void buildDependency(FlowDef def, Config config) {
				Task header = Task.newTask("msgConsumer")
						.setExecutor(new HiveJobHistorySourcedStormSpoutProvider())
						.completeBuild();
				Task hiveQueryParser = Task.newTask("hiveQueryParser")
						.setExecutor(new HiveQueryParserExecutor())
						.connectFrom(header).completeBuild();
				Task hiveResourceSensitivityLookup = Task.newTask("hiveResourceSensitivityLookup")
						.setExecutor(new HiveResourceSensitivityDataJoinExecutor())
						.connectFrom(hiveQueryParser).completeBuild();

				try {							
					hiveQueryAlertTasks = AlertExecutorCreationUtils.setupDefaultAlerts(
							config, hiveResourceSensitivityLookup);
				} catch(Exception ex){
					LOG.error("Fail creating alert executors", ex);
					throw new IllegalStateException(ex);
				}
				
				def.endBy(hiveQueryAlertTasks.toArray(new Task[0]));
			}

			@Override
			protected void buildDeploymentDescriptor(AbstractTaskGraph graph,
					Config config) {
                Iterator<Task> iterator = graph.iterator();
                while (iterator.hasNext()) {
                    Task task = iterator.next();
                    Task[] previousTasks = task.getPrevious();
                    if (previousTasks != null) {
                        for (Task previousTask : previousTasks) {
                            if (previousTask.getName().equals("hiveResourceSensitivityLookup")) {
                                graph.updateTaskDataExchangeProtocol(
                                		previousTask.getName(),
                                		task.getName(),
                                		StormDataExProto.newProto().enableEventPartition());
                            }
                            else {
                                graph.updateTaskDataExchangeProtocol(
                                		previousTask.getName(),
                                		task.getName(),
                                		StormDataExProto.newProto());
                            }
                        }
                    }
                }
			}
		};

		executor.execute();
	}*/

	public static void main(String[] args) throws Exception{

		System.setProperty("config.resource", "/data-process-runtime-hive-jobhistory.conf");
		LOG.info("start hive query log monitoring program ...");

		//new ConfigOptionParser().load(args);
		//System.setProperty("config.trace", "loads");
		Config config = ConfigFactory.load();

		LOG.info("Config class: " + config.getClass().getCanonicalName());

		if(LOG.isDebugEnabled()) LOG.debug("Config content:"+config.root().render(ConfigRenderOptions.concise()));

		String spoutName = "msgConsumer";
		int parallelism = config.getInt("envContextConfig.parallelismConfig." + spoutName);
		StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
		env.newSource(new HiveJobHistorySourcedStormSpoutProvider().getSpout(config, parallelism)).renameOutputFields(3).withName(spoutName)
				.flatMap(new HiveQueryParserExecutor())
				.flatMap(new HiveResourceSensitivityDataJoinExecutor())
				.alertWithConsumer("hiveAccessLogStream", "hiveAccessAlertByRunningJob");
		env.execute();
	}
}
