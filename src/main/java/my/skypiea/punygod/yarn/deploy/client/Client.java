package my.skypiea.punygod.yarn.deploy.client;

import my.skypiea.punygod.yarn.deploy.util.Constants;
import my.skypiea.punygod.yarn.deploy.util.ProcessRunner;
import my.skypiea.punygod.yarn.deploy.util.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Interact with YARN cluster on client command and options.
 */
public class Client extends ProcessRunner {
    private static final Logger LOG = LogManager.getLogger(Client.class);

    private final YarnClient yarnClient;
    private static final String LAUNCH = "launch";
    private static final String CLUSTER = "cluster";
    private final Configuration conf;
    private String cmdName;
    private Command command;

    public Client() {
        this(new YarnConfiguration(), YarnClient.createYarnClient());
    }

    Client(Configuration conf, YarnClient yarnClient) {
        super("Client");
        this.conf = conf;
        this.yarnClient = yarnClient;
        yarnClient.init(conf);
        yarnClient.start();
        LOG.info("Starting YarnClient...");
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.run(args);
    }


    @Override
    public void init(CommandLine cliParser) {
        if (cmdName.equalsIgnoreCase(LAUNCH)) {
            command = new LaunchCluster(conf, yarnClient, cliParser);
        }
    }

    @Override
    public Options initOptions(String[] args) {
        Options options = new Options();
        if (args.length > 0) {
            cmdName = args[0];
            if (cmdName.equalsIgnoreCase(LAUNCH)) {
                Utils.addClientOptions(options);
            } else if (cmdName.equalsIgnoreCase(CLUSTER)) {
                options.addOption(Constants.OPT_APPLICATION_ID, true, "Application ID");
            }
        }

        return options;
    }

    @Override
    public boolean run() throws Exception {
        return command.run();
    }


    interface Command {
        boolean run() throws Exception;
    }
}
