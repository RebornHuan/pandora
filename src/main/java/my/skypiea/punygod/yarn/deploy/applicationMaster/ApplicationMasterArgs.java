package my.skypiea.punygod.yarn.deploy.applicationMaster;

import my.skypiea.punygod.yarn.deploy.util.Constants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ApplicationMasterArgs {

  private static final Log LOG = LogFactory.getLog(ApplicationMasterArgs.class);
  private final long containerMemory;
  private final int containerVCores;
  final int totalContainerNum;
  final String dataxTar;

  public ApplicationMasterArgs(CommandLine cliParser) {
    containerMemory = Long.parseLong(cliParser.getOptionValue(
        Constants.OPT_DATAX_CONTAINER_MEMORY, Constants.DEFAULT_CONTAINER_MEMORY));
    containerVCores = Integer.parseInt(cliParser.getOptionValue(
        Constants.OPT_DATAX_CONTAINER_VCORES, Constants.DEFAULT_CONTAINER_VCORES));

    totalContainerNum = 1;

//    if (!cliParser.hasOption(Constants.OPT_DATAX_TAR)) {
//      throw new IllegalArgumentException("No Datax tar specified");
//    }
//    dataxTar = cliParser.getOptionValue(Constants.OPT_DATAX_TAR);
    dataxTar = "hdfs://nn-idc/tmp/datax.tar.gz";
  }

  int getContainerVCores(int maxVCores) {
    if (containerVCores > maxVCores) {
      LOG.warn("Container vcores specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerVCores + ", max="
          + maxVCores);
      return maxVCores;
    } else {
      return containerVCores;
    }
  }

  long getContainerMemory(long maxMem) {
    if (containerMemory > maxMem) {
      LOG.warn("Container memory specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerMemory + ", max="
          + maxMem);
      return maxMem;
    } else {
      return containerMemory;
    }
  }
}
