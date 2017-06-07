package my.skypiea.punygod.yarn.deploy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static my.skypiea.punygod.yarn.deploy.util.URL.*;

public class Utils {

    private static final Log LOG = LogFactory.getLog(Utils.class);

    public static Map<String, String> setJavaEnv(Configuration conf) {
        Map<String, String> env = new HashMap<>();

        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    public static Map<String, LocalResource> makeLocalResourcesFile(
            FileSystem fs, Map<String, Path> files) throws IOException {
        Map<String, LocalResource> localResources = new HashMap<>();
        for (Map.Entry<String, Path> entry : files.entrySet()) {
            addToLocalResources(fs, entry.getKey(), entry.getValue(), LocalResourceType.FILE,localResources);
        }
        return localResources;
    }

    public static Map<String, LocalResource> makeLocalResourcesArchive(
            FileSystem fs, Map<String, Path> files) throws IOException {
        Map<String, LocalResource> localResources = new HashMap<>();
        for (Map.Entry<String, Path> entry : files.entrySet()) {
            addToLocalResources(fs, entry.getKey(), entry.getValue(), LocalResourceType.ARCHIVE,localResources);
        }
        return localResources;
    }

    private static void addToLocalResources(FileSystem fs, String key, Path dst,LocalResourceType localResourceType,
                                            Map<String, LocalResource> localResources) throws IOException {
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource resource =
                LocalResource.newInstance(
                        fromURI(dst.toUri()),
                        localResourceType, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(key, resource);
    }

    public static Path copyLocalFileToDfs(FileSystem fs, String appId,
                                          Path srcPath, String dstFileName) throws IOException {
        Path dstPath = new Path(fs.getHomeDirectory(),
                Constants.DEFAULT_APP_NAME + Path.SEPARATOR + appId + Path.SEPARATOR + dstFileName);
        LOG.info("Copying " + srcPath + " to " + dstPath);
        fs.copyFromLocalFile(srcPath, dstPath);
        return dstPath;
    }

    public static String mkString(String[] list, String separator) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < list.length; i++) {
            builder.append(list[i]);
            if (i < list.length - 1) {
                builder.append(separator);
            }
        }
        return builder.toString();
    }

    public static String mkOption(String option, Object value) {
        return "--" + option + " " + value;
    }


    public static void addClientOptions(Options opts) {
        opts.addOption(
                Constants.OPT_DATAX_APP_NAME, true,
                "Application Name. Default value " + Constants.DEFAULT_APP_NAME);
        opts.addOption(Constants.OPT_DATAX_APP_MASTER_MEMORY, true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption(Constants.OPT_DATAX_APP_MASTER_VCORES, true,
                "Amount of virtual cores to be requested to run the application master");
        addAppMasterOptions(opts);
    }

    public static void addAppMasterOptions(Options opts) {
        addContainerOptions(opts);
    }

    public static void addContainerOptions(Options opts) {
        opts.addOption(Constants.OPT_DATAX_CONTAINER_MEMORY, true,
                "Amount of memory in MB to be requested to run a container");
        opts.addOption(Constants.OPT_DATAX_CONTAINER_VCORES, true,
                "Amount of virtual cores to be requested to run a container");
        opts.addOption(Constants.OPT_DATAX_TAR, true,
                "Jar file containing");
    }

    public static String toJsonString(Object object) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(object);
    }


    public static String getParentDir(String path) {
        File file = new File(path);
        return file.getParent();
    }


}
