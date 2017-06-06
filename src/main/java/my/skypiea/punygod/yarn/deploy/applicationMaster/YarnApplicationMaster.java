package my.skypiea.punygod.yarn.deploy.applicationMaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class YarnApplicationMaster implements AMRMClientAsync.CallbackHandler {
    private static final Logger logger = LogManager.getLogger(YarnApplicationMaster.class);

    @Override
    public void onContainersCompleted(final List<ContainerStatus> list) {

    }

    @Override
    public void onContainersAllocated(final List<Container> list) {

    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(final List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(final Throwable throwable) {

    }

    private static Configuration yarnConfiguration;
    private static Resource executorResource;
    private static Priority executorPriority;

    public static void main(String args[]) {
        yarnConfiguration = new YarnConfiguration();
        executorResource = Records.newRecord(Resource.class);
        executorPriority = Records.newRecord(Priority.class);
    }
}
