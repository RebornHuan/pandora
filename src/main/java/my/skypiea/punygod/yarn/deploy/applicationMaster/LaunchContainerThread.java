package my.skypiea.punygod.yarn.deploy.applicationMaster;

import com.google.common.collect.Lists;
import my.skypiea.punygod.yarn.deploy.util.Constants;
import my.skypiea.punygod.yarn.deploy.util.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.util.HashMap;
import java.util.Map;

public class LaunchContainerThread extends Thread {
    private static final Log LOG = LogFactory.getLog(LaunchContainerThread.class);
    private final Container container;
    private final String dataxJar;
    private final long containerMemory;
    private final ApplicationMaster appMaster;

    public LaunchContainerThread(Container container, ApplicationMaster appMaster,
                                 long containerMemory,
                                 String dataxJar) {
        this.container = container;
        this.appMaster = appMaster;
        this.containerMemory = containerMemory;
        this.dataxJar = dataxJar;
    }

    @Override
    public void run() {
        try {
            Map<String, String> env = Utils.setJavaEnv(appMaster.getConfiguration());
            String current = ApplicationConstants.Environment.LD_LIBRARY_PATH.$$();
            env.put("LD_LIBRARY_PATH", current + ":" + "`pwd`");

            Map<String, Path> files = new HashMap<>();
            files.put(Constants.DATAX_JAR_NAME, new Path(dataxJar));

            FileSystem fs = FileSystem.get(appMaster.getConfiguration());
            Map<String, LocalResource> localResources =
                    Utils.makeLocalResources(fs, files);

            String command = makeContainerCommand(
                    containerMemory, "test");

            LOG.info("Launching a new container."
                    + ", containerId=" + container.getId()
                    + ", containerNode=" + container.getNodeId().getHost()
                    + ":" + container.getNodeId().getPort()
                    + ", containerNodeURI=" + container.getNodeHttpAddress()
                    + ", containerResourceMemory="
                    + container.getResource().getMemorySize()
                    + ", containerResourceVirtualCores="
                    + container.getResource().getVirtualCores()
                    + ", command: " + command);
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, env, Lists.newArrayList(command), null, null, null);
            appMaster.addContainer(container);
            appMaster.getNMClientAsync().startContainerAsync(container, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String makeContainerCommand(long containerMemory,
                                        String jobName) {
        String[] commands = new String[]{
                ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",
                "-Xmx" + containerMemory + "m",
                " ",
                Utils.mkOption(Constants.OPT_JOB_NAME, jobName),
                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        ApplicationConstants.STDOUT,
                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        ApplicationConstants.STDERR
        };

        return Utils.mkString(commands, " ");
    }

}