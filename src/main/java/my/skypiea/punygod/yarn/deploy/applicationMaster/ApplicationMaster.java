
package my.skypiea.punygod.yarn.deploy.applicationMaster;

import my.skypiea.punygod.yarn.deploy.util.ProcessRunner;
import my.skypiea.punygod.yarn.deploy.util.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster extends ProcessRunner {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    private final Set<ContainerId> launchedContainers =
            Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
    private final Set<Container> allocatedContainers =
            Collections.newSetFromMap(new ConcurrentHashMap<Container, Boolean>());
    private Configuration conf;
    private ApplicationAttemptId appAttemptId;
    private ApplicationMasterArgs args;
    private int containerMemory;
    private int containerVCores;
    private AMRMClientAsync amRMClient;
    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;
    private volatile boolean done;
    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger completedContainerNum = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    private AtomicInteger allocatedContainerNum = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger failedContainerNum = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    private AtomicInteger requestedContainerNum = new AtomicInteger();
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<>();

    private final Credentials credentials;

    public ApplicationMaster() throws IOException {
        super("ApplicationMaster");
        conf = new YarnConfiguration();
        credentials = UserGroupInformation.getCurrentUser().getCredentials();

    }

    public static void main(String[] args) throws IOException {
        ApplicationMaster appMaster = new ApplicationMaster();
        appMaster.run(args);

    }

    @Override
    public Options initOptions(String[] args) {
        Options options = new Options();
        Utils.addAppMasterOptions(options);
        return options;
    }

    @Override
    public void init(CommandLine cliParser) throws Exception {
        LOG.info("Starting ApplicationMaster");

        // Args have to be initialized first
        this.args = new ApplicationMasterArgs(cliParser);

        String hostname = System.getenv(Environment.NM_HOST.name());
        int rpcPort = 0;
        RegisterApplicationMasterResponse response = setupRMConnection(hostname, rpcPort);
        setupPreviousRunningContainers(response);
        setupContainerResource(response);
        setupNMConnection();
    }

    /**
     * Main run function for the application master
     */
    @SuppressWarnings({"unchecked"})
    public boolean run() throws Exception {

        int numTotalContainersToRequest =
                args.totalContainerNum - launchedContainers.size();
        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }
        requestedContainerNum.set(args.totalContainerNum);

        // wait for completion.
        while (!done
                && (completedContainerNum.get() != args.totalContainerNum)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOG.error("Exception thrown when waiting for container completion: " + e.getMessage());
                throw e;
            }
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.error("Exception thrown in thread join: " + e.getMessage());
                throw e;
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus = getFinalAppStatus();
        String appMessage = "Diagnostics." + ", total=" + args.totalContainerNum
                + ", completed=" + completedContainerNum.get() + ", allocated="
                + allocatedContainerNum.get() + ", failed="
                + failedContainerNum.get();
        LOG.info(appMessage);
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (Exception ex) {
            LOG.error("Failed to unregister application", ex);
        }
        amRMClient.stop();
        return appStatus.equals(FinalApplicationStatus.SUCCEEDED);
    }

    NMClientAsync getNMClientAsync() {
        return nmClientAsync;
    }

    Configuration getConfiguration() {
        return conf;
    }

    void addContainer(Container container) {
        containerListener.addContainer(container.getId(), container);
    }

    private RegisterApplicationMasterResponse setupRMConnection(String hostname, int rpcPort) throws Exception {
        AMRMClientAsync.CallbackHandler allocListener =
                new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();
        // Register self with ResourceManager
        // This will start heartbeating to the RM
        return amRMClient.registerApplicationMaster(hostname, rpcPort, "");
    }

    private void setupNMConnection() {
        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();
    }

    private void setupPreviousRunningContainers(RegisterApplicationMasterResponse response) {
        String containerIdStr =
                System.getenv(Environment.CONTAINER_ID.name());
        ContainerId containerId = ContainerId.fromString(containerIdStr);
        appAttemptId = containerId.getApplicationAttemptId();
        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptId + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        for (Container container : previousAMRunningContainers) {
            launchedContainers.add(container.getId());
        }
        allocatedContainerNum.addAndGet(previousAMRunningContainers.size());
    }

    private void setupContainerResource(RegisterApplicationMasterResponse response) {
        // Dump out information about cluster capability as seen by the
        // resource manager
        long maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capability of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

        this.containerMemory = (int) args.getContainerMemory(maxMem);
        this.containerVCores = args.getContainerVCores(maxVCores);
    }


    private FinalApplicationStatus getFinalAppStatus() {
        if (completedContainerNum.get() - failedContainerNum.get() >= args.totalContainerNum) {
            return FinalApplicationStatus.SUCCEEDED;
        } else {
            return FinalApplicationStatus.FAILED;
        }
    }

    private void startAllContainers() throws IOException {
        for (Container allocatedContainer : allocatedContainers) {
            launchContainer(allocatedContainer);
        }
    }

    private void launchContainer(Container container) {
        LaunchContainerThread launchThread = new LaunchContainerThread(container,
                this, containerMemory, args.dataxTar, credentials);
        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchedContainers.add(container.getId());
        launchThread.start();
    }

    private String getAddress(String hostName, int port) {
        return hostName + ":" + port;
    }


    private int genNextPort(int port) {
        return port + 2;
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(containerMemory, containerVCores);
        Priority priority = Priority.newInstance(0);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                priority);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private final ApplicationMaster applicationMaster;
        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();

        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(
                        containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.completedContainerNum.incrementAndGet();
            applicationMaster.failedContainerNum.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }

    }


    class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptId + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);
                // ignore containers we know nothing about - probably from a previous
                // attempt
                if (!launchedContainers.contains(containerStatus.getContainerId())) {
                    LOG.info("Ignoring completed status of "
                            + containerStatus.getContainerId()
                            + "; unknown container(probably launched by previous attempt)");
                    continue;
                }

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        completedContainerNum.incrementAndGet();
                        failedContainerNum.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        allocatedContainerNum.decrementAndGet();
                        requestedContainerNum.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    completedContainerNum.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            // ask for more containers if any failed
            int askCount = args.totalContainerNum - requestedContainerNum.get();
            requestedContainerNum.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClient.addContainerRequest(containerAsk);
                }
            }

            if (completedContainerNum.get() == args.totalContainerNum) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            allocatedContainerNum.addAndGet(allocatedContainers.size());
            ApplicationMaster.this.allocatedContainers.addAll(allocatedContainers);
            if (allocatedContainerNum.get() == args.totalContainerNum) {
                try {
                    startAllContainers();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            return (float) completedContainerNum.get() / args.totalContainerNum;
        }

        @Override
        public void onError(Throwable e) {
            LOG.error("Error in RMCallbackHandler: ", e);
            done = true;
            amRMClient.stop();
        }
    }

}
