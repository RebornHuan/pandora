/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package my.skypiea.punygod.yarn.deploy.util;

import my.skypiea.punygod.yarn.deploy.client.Client;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstracts common methods for running process like {@link Client}.
 */
public abstract class ProcessRunner {

    private static final Logger LOG = LogManager.getLogger(ProcessRunner.class);
    private final String name;

    protected ProcessRunner(String name) {
        this.name = name;
    }

    public void run(String[] args) {
        boolean success = false;
        LOG.info("Initializing " + name);
        Options opts = initOptions(args);
        try {
            init(parseArgs(opts, args));
            success = run();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getLocalizedMessage());
            printUsage(opts);
            System.exit(-1);
        } catch (Throwable t) {
            LOG.fatal("Error running " + name, t);
            System.exit(1);
        }

        if (success) {
            System.exit(0);
        } else {
            LOG.error(name + " failed to complete successfully");
            System.exit(2);
        }
    }

    public abstract Options initOptions(String[] args);

    public abstract void init(CommandLine cliParser) throws Exception;

    public abstract boolean run() throws Exception;

    private CommandLine parseArgs(Options opts, String[] args) throws ParseException {
        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for " + name + " to initialize");
        }

        return new GnuParser().parse(opts, args);
    }

    private void printUsage(Options opts) {
        new HelpFormatter().printHelp(name, opts);
    }
}
