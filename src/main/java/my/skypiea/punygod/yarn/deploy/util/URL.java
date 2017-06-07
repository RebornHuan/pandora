package my.skypiea.punygod.yarn.deploy.util;


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * <p><code>URL</code> represents a serializable {@link java.net.URL}.</p>
 */
@Public
@Stable
public abstract class URL extends org.apache.hadoop.yarn.api.records.URL {

    @Public
    @Stable
    public static URL newInstance(String scheme, String host, int port, String file) {
        URL url = Records.newRecord(URL.class);
        url.setScheme(scheme);
        url.setHost(host);
        url.setPort(port);
        url.setFile(file);
        return url;
    }

    /**
     * Get the scheme of the URL.
     * @return scheme of the URL
     */
    @Public
    @Stable
    public abstract String getScheme();

    /**
     * Set the scheme of the URL
     * @param scheme scheme of the URL
     */
    @Public
    @Stable
    public abstract void setScheme(String scheme);

    /**
     * Get the user info of the URL.
     * @return user info of the URL
     */
    @Public
    @Stable
    public abstract String getUserInfo();

    /**
     * Set the user info of the URL.
     * @param userInfo user info of the URL
     */
    @Public
    @Stable
    public abstract void setUserInfo(String userInfo);

    /**
     * Get the host of the URL.
     * @return host of the URL
     */
    @Public
    @Stable
    public abstract String getHost();

    /**
     * Set the host of the URL.
     * @param host host of the URL
     */
    @Public
    @Stable
    public abstract void setHost(String host);

    /**
     * Get the port of the URL.
     * @return port of the URL
     */
    @Public
    @Stable
    public abstract int getPort();

    /**
     * Set the port of the URL
     * @param port port of the URL
     */
    @Public
    @Stable
    public abstract void setPort(int port);

    /**
     * Get the file of the URL.
     * @return file of the URL
     */
    @Public
    @Stable
    public abstract String getFile();

    /**
     * Set the file of the URL.
     * @param file file of the URL
     */
    @Public
    @Stable
    public abstract void setFile(String file);

    @Public
    @Stable
    public Path toPath() throws URISyntaxException {
        return new Path(new URI(getScheme(), getUserInfo(),
                getHost(), getPort(), getFile(), null, null));
    }


    @Private
    public static org.apache.hadoop.yarn.api.records.URL fromURI(URI uri, Configuration conf) {
        org.apache.hadoop.yarn.api.records.URL url =
                RecordFactoryProvider.getRecordFactory(conf).newRecordInstance(
                        org.apache.hadoop.yarn.api.records.URL.class);
        if (uri.getHost() != null) {
            url.setHost(uri.getHost());
        }
        if (uri.getUserInfo() != null) {
            url.setUserInfo(uri.getUserInfo());
        }
        url.setPort(uri.getPort());
        url.setScheme(uri.getScheme());
        url.setFile(uri.getPath());
        return url;
    }

    @Public
    @Stable
    public static org.apache.hadoop.yarn.api.records.URL fromURI(URI uri) {
        return fromURI(uri, null);
    }

    @Private
    public static org.apache.hadoop.yarn.api.records.URL fromPath(Path path, Configuration conf) {
        return fromURI(path.toUri(), conf);
    }

    @Public
    @Stable
    public static org.apache.hadoop.yarn.api.records.URL fromPath(Path path) {
        return fromURI(path.toUri());
    }
}
