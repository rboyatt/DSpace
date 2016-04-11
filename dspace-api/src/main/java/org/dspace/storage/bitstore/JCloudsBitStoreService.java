package org.dspace.storage.bitstore;

import com.google.common.io.ByteSource;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.jclouds.*;
import org.jclouds.blobstore.*;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.io.ByteSources;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dspace.content.Bitstream;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Utils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class JCloudsBitStoreService implements BitStoreService {

    /** log4j log */
    private static Logger log = Logger.getLogger(JCloudsBitStoreService.class);

    /** Checksum algorithm */
    private static final String CSA = "MD5";

    private String blobAccessKey;
    private String blobSecretKey;

    /** container for all the assets */
    private String bucketName = null;

    /** (Optional) subfolder within bucket where objects are stored */
    private String subfolder = null;

    /** JClouds backend settings */
    private String backendName = null;
    private String endpointURL = null;

    private BlobStoreContext context = null;
    private BlobStore blobStore = null;

    public JCloudsBitStoreService()
    {
    }

    public void init() throws IOException
    {
        if(StringUtils.isBlank(getAccessKey()) || StringUtils.isBlank(getAccessKey())) {
            log.warn("Empty access or secret");
        }

        // bucket name
        if(StringUtils.isEmpty(bucketName)) {
            bucketName = "dspace-asset-" + ConfigurationManager.getProperty("dspace.hostname");
            log.warn("BucketName is not configured, setting default: " + bucketName);
        }

        context = ContextBuilder.newBuilder(backendName)
                .endpoint(endpointURL)
                .credentials(blobAccessKey, blobSecretKey)
                .buildView(BlobStoreContext.class);

        blobStore = context.getBlobStore();

        try {
            if(! blobStore.containerExists(bucketName)) {
                blobStore.createContainerInLocation(null, bucketName);
                log.info("Creating new bucket: " + bucketName);
            }
        }
        catch (Exception e)
        {
            log.error(e);
            throw new IOException(e);
        }
    }

    /**
     * Return an identifier unique to this asset store instance
     *
     * @return a unique ID
     */
    public String generateId()
    {
        return Utils.generateKey();
    }

    /**
     * Retrieve the bits for the asset with ID. If the asset does not
     * exist, returns null.
     *
     * @param bitstream
     *            The ID of the asset to retrieve
     * @exception java.io.IOException
     *                If a problem occurs while retrieving the bits
     *
     * @return The stream of bits, or null
     */
    public InputStream get(Bitstream bitstream) throws IOException
    {
        String key = getFullKey(bitstream.getInternalId());
        try
        {
            Blob object = blobStore.getBlob(bucketName, getFullKey(key));
            return (object != null) ? object.getPayload().openStream() : null;
        }
        catch (Exception e)
        {
            log.error("get("+key+")", e);
            throw new IOException(e);
        }
    }

    /**
     * Store a stream of bits.
     *
     * <p>
     * If this method returns successfully, the bits have been stored.
     * If an exception is thrown, the bits have not been stored.
     * </p>
     *
     * @param in
     *            The stream of bits to store
     * @exception java.io.IOException
     *             If a problem occurs while storing the bits
     *
     * @return Map containing technical metadata (size, checksum, etc)
     */
    public void put(Bitstream bitstream, InputStream in) throws IOException
    {
        String key = getFullKey(bitstream.getInternalId());
        File scratchFile = File.createTempFile(bitstream.getInternalId(), "s3bs");

        //Copy istream to temp file, and send the file, with some metadata
        try {
            FileUtils.copyInputStreamToFile(in, scratchFile);
            Long contentLength = scratchFile.length();

            Blob blob = blobStore.blobBuilder(key)
                    .payload(in)
                    .contentLength(contentLength)
                    .build();

            String etag = blobStore.putBlob(bucketName, blob);

            bitstream.setSizeBytes(contentLength);
            bitstream.setChecksum(etag);
            bitstream.setChecksumAlgorithm(CSA);

            scratchFile.delete();

        } catch(Exception e) {
            log.error("put(" + bitstream.getInternalId() +", is)", e);
            throw new IOException(e);
        } finally {
            if(scratchFile.exists()) {
                scratchFile.delete();
            }
        }
    }

    /**
     * Obtain technical metadata about an asset in the asset store.
     *
     * Checksum used is (ETag) hex encoded 128-bit MD5 digest of an object's content as calculated by Amazon S3
     * (Does not use getContentMD5, as that is 128-bit MD5 digest calculated on caller's side)
     *
     * @param bitstream
     *            The asset to describe
     * @param attrs
     *            A Map whose keys consist of desired metadata fields
     *
     * @exception java.io.IOException
     *            If a problem occurs while obtaining metadata
     * @return attrs
     *            A Map with key/value pairs of desired metadata
     *            If file not found, then return null
     */
    public Map about(Bitstream bitstream, Map attrs) throws IOException
    {
        String key = getFullKey(bitstream.getInternalId());
        try {
            BlobMetadata objectMetadata = blobStore.blobMetadata(bucketName, key);

            if (objectMetadata != null) {
                if (attrs.containsKey("size_bytes")) {
                    attrs.put("size_bytes", objectMetadata.getSize());
                }
                if (attrs.containsKey("checksum")) {
                    attrs.put("checksum", objectMetadata.getETag());
                    attrs.put("checksum_algorithm", CSA);
                }
                if (attrs.containsKey("modified")) {
                    attrs.put("modified", String.valueOf(objectMetadata.getLastModified().getTime()));
                }
                return attrs;
            }
        } catch (ContainerNotFoundException e) {
            return null;
        } catch (Exception e) {
            log.error("about("+key+", attrs)", e);
            throw new IOException(e);
        }
        return null;
    }

    /**
     * Remove an asset from the asset store. An irreversible operation.
     *
     * @param bitstream
     *            The asset to delete
     * @exception java.io.IOException
     *             If a problem occurs while removing the asset
     */
    public void remove(Bitstream bitstream) throws IOException
    {
        String key = getFullKey(bitstream.getInternalId());
        try {
            blobStore.removeBlob(bucketName, getFullKey(key));
        } catch (Exception e) {
            log.error("remove("+key+")", e);
            throw new IOException(e);
        }
    }

    /**
     * Utility Method: Prefix the key with a subfolder, if this instance assets are stored within subfolder
     * @param id
     * @return
     */
    public String getFullKey(String id) {
        if(StringUtils.isNotEmpty(subfolder)) {
            return subfolder + "/" + id;
        } else {
            return id;
        }
    }

    public String getAccessKey() {
        return blobAccessKey;
    }

    @Required
    public void setAccessKey(String blobAccessKey) {
        this.blobAccessKey = blobAccessKey;
    }

    public String getSecretKey() {
        return blobSecretKey;
    }

    @Required
    public void setSecretKey(String blobSecretKey) {
        this.blobSecretKey = blobSecretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    @Required
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getSubfolder() {
        return subfolder;
    }

    public void setSubfolder(String subfolder) {
        this.subfolder = subfolder;
    }

    public String getBackendName() { return backendName; }

    @Required
    public void setBackendName(String backendName) {
        this.backendName = backendName;
    }

    public String getEndpointURL() { return endpointURL; }

    @Required
    public void setEndpointURL(String endpointURL) {
        this.endpointURL = endpointURL;
    }

    /**
     * Contains a command-line testing tool. Expects arguments:
     *  -a accessKey -s secretKey -f assetFileName
     *
     * @param args
     *        Command line arguments
     */
    public static void main(String[] args) throws Exception
    {

    }
}
