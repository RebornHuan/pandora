package my.skypiea.punygod.yarn.deploy.applicationMaster;

import com.google.common.collect.Lists;
import my.skypiea.punygod.yarn.deploy.util.Constants;
import my.skypiea.punygod.yarn.deploy.util.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LaunchContainerThread extends Thread {
    private static final Log LOG = LogFactory.getLog(LaunchContainerThread.class);
    private final Container container;
    private final String dataxTar;
    private final long containerMemory;
    private final ApplicationMaster appMaster;

    // user credentials
    private final Credentials credentials;

    public LaunchContainerThread(Container container, ApplicationMaster appMaster,
                                 long containerMemory,
                                 String dataxJar, Credentials credentials) {
        this.container = container;
        this.appMaster = appMaster;
        this.containerMemory = containerMemory;
        this.dataxTar = dataxJar;
        this.credentials = credentials;

    }

    @Override
    public void run() {
        try {
            Map<String, String> env = Utils.setJavaEnv(appMaster.getConfiguration());
            String current = ApplicationConstants.Environment.LD_LIBRARY_PATH.$$();
            env.put("LD_LIBRARY_PATH", current + ":" + "`pwd`");

            Map<String, Path> files = new HashMap<>();
            files.put(Constants.DATAX_FOLDER, new Path(dataxTar));

            FileSystem fs = FileSystem.get(appMaster.getConfiguration());
            Map<String, LocalResource> localResources =
                    Utils.makeLocalResourcesArchive(fs, files);

            String command = makeContainerCommand();

            LOG.info("Launching a new container."
                    + ", containerId=" + container.getId()
                    + ", containerNode=" + container.getNodeId().getHost()
                    + ":" + container.getNodeId().getPort()
                    + ", containerNodeURI=" + container.getNodeHttpAddress()
                    + ", containerResourceMemory="
                    + container.getResource().getMemory()
                    + ", containerResourceVirtualCores="
                    + container.getResource().getVirtualCores()
                    + ", command: " + command);
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, env, Lists.newArrayList(command), null, setupTokens(), null);
            appMaster.addContainer(container);
            appMaster.getNMClientAsync().startContainerAsync(container, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * setup security token given current user
     *
     * @return the ByeBuffer containing the security tokens
     * @throws IOException
     */
    private ByteBuffer setupTokens() {
        try {
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            return ByteBuffer.wrap(dob.getData(), 0, dob.getLength()).duplicate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String makeContainerCommand() {
        String[] commands = new String[]{
                "cd datax_folder/datax; bin/datax.py job/job.json",
                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        "/datax.stdout",
                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        "/datax.stderr"
        };

        return Utils.mkString(commands, " ");
    }

}