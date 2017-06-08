
package my.skypiea.punygod.yarn.deploy.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import my.skypiea.punygod.yarn.deploy.applicationMaster.ApplicationMaster;
import my.skypiea.punygod.yarn.deploy.util.Constants;
import my.skypiea.punygod.yarn.deploy.util.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class LaunchCluster implements Client.Command {
    private static final Log LOG = LogFactory.getLog(LaunchCluster.class);
    private final Configuration conf;
    private final YarnClient yarnClient;
    private final String appName;
    private final Integer amMemory;
    private final Integer amVCores;
    private final String amQueue;
    private final Integer containerMemory;
    private final Integer containerVCores;
    private final String pandoraJar;
    private final Credentials credentials;


    public LaunchCluster(Configuration conf, YarnClient yarnClient, CommandLine cliParser, Credentials credentials) {
        this.conf = conf;
        this.yarnClient = yarnClient;

        this.credentials =  credentials;
        appName = cliParser.getOptionValue(
                Constants.OPT_DATAX_APP_NAME, Constants.DEFAULT_APP_NAME);

        amMemory = Integer.parseInt(cliParser.getOptionValue(
                Constants.OPT_DATAX_APP_MASTER_MEMORY, Constants.DEFAULT_APP_MASTER_MEMORY));
        amVCores = Integer.parseInt(cliParser.getOptionValue(
                Constants.OPT_DATAX_APP_MASTER_VCORES, Constants.DEFAULT_APP_MASTER_VCORES));
        amQueue = cliParser.getOptionValue(
                Constants.OPT_DATAX_APP_MASTER_QUEUE, Constants.DEFAULT_APP_MASTER_QUEUE);
        containerMemory = Integer.parseInt(cliParser.getOptionValue(
                Constants.OPT_DATAX_CONTAINER_MEMORY, Constants.DEFAULT_CONTAINER_MEMORY));
        containerVCores = Integer.parseInt(cliParser.getOptionValue(
                Constants.OPT_DATAX_CONTAINER_VCORES, Constants.DEFAULT_CONTAINER_VCORES));

        if (cliParser.hasOption(Constants.OPT_PANDORA_JAR)) {
            pandoraJar = cliParser.getOptionValue(Constants.OPT_PANDORA_JAR);
        } else {
            pandoraJar = ClassUtil.findContainingJar(getClass());
        }
    }

    public boolean run() throws Exception {
        YarnClientApplication app = createApplication();
        ApplicationId appId = app.getNewApplicationResponse().getApplicationId();

        // Copy the application jar to the filesystem
        FileSystem fs = FileSystem.get(conf);

        String appIdStr = appId.toString();
        Path dstJarPath = Utils.copyLocalFileToDfs(fs, appIdStr, new Path(pandoraJar), Constants.PANDORA_JAR_NAME);
        Map<String, Path> files = new HashMap<>();
        files.put(Constants.PANDORA_JAR_NAME, dstJarPath);

        Map<String, LocalResource> localResources = Utils.makeLocalResourcesFile(fs, files);
        Map<String, String> javaEnv = Utils.setJavaEnv(conf);
        String command = makeAppMasterCommand(dstJarPath.toString());
        LOG.info("Make ApplicationMaster command: " + command);
        ContainerLaunchContext launchContext = ContainerLaunchContext.newInstance(
                localResources, javaEnv, Lists.newArrayList(command), null, setupTokens(fs), null);
        Resource resource = Resource.newInstance(amMemory, amVCores);
        submitApplication(app, appName, launchContext, resource, amQueue);
        return awaitApplication(appId);
    }


    /**
     * setup security token given current user
     * @return the ByeBuffer containing the security tokens
     * @throws IOException
     */
    private ByteBuffer setupTokens(FileSystem fs) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        String loc = System.getenv().get("HADOOP_TOKEN_FILE_LOCATION");
        if ((loc != null && loc.trim().length() > 0)
                ||  (!UserGroupInformation.isSecurityEnabled())) {
            this.credentials.writeTokenStorageToStream(buffer);
        } else {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }
            // For now, only getting tokens for the default file-system.
            final org.apache.hadoop.security.token.Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (org.apache.hadoop.security.token.Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            credentials.writeTokenStorageToStream(buffer);
        }
        return ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength());
    }

    YarnClientApplication createApplication() throws Exception {
        return yarnClient.createApplication();
    }

    ApplicationId submitApplication(
            YarnClientApplication app,
            String appName,
            ContainerLaunchContext launchContext,
            Resource resource,
            String queue) throws Exception {
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(appName);
        appContext.setApplicationTags(new HashSet<String>());
        appContext.setAMContainerSpec(launchContext);
        appContext.setResource(resource);
        appContext.setQueue(queue);

        return yarnClient.submitApplication(appContext);
    }

    boolean awaitApplication(ApplicationId appId) throws Exception {
        Set<YarnApplicationState> terminated = Sets.newHashSet(
                YarnApplicationState.FAILED,
                YarnApplicationState.FINISHED,
                YarnApplicationState.KILLED);
        while (true) {
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            if (state.equals(YarnApplicationState.RUNNING)) {
            } else if (terminated.contains(state)) {
                return false;
            } else {
                Thread.sleep(1000);
            }
        }
    }

    private String makeAppMasterCommand(String dataxJar) {
        String[] commands = new String[]{
                ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java",
                // Set Xmx based on am memory size
                "-Xmx" + amMemory + "m",
                // Set class name
                ApplicationMaster.class.getName(),
                Utils.mkOption(Constants.OPT_DATAX_CONTAINER_MEMORY, containerMemory),
                Utils.mkOption(Constants.OPT_DATAX_CONTAINER_VCORES, containerVCores),
                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout",
                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr"
        };
        return Utils.mkString(commands, " ");
    }
}
