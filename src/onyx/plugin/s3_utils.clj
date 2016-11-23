(ns onyx.plugin.s3-utils
  (:import [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.handlers AsyncHandler]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.services.s3.model ObjectMetadata]
           [java.io ByteArrayInputStream]
           [com.amazonaws.services.s3.transfer TransferManager Upload]
           [com.amazonaws.event ProgressListener$ExceptionReporter]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [org.apache.commons.codec.digest DigestUtils]
           [com.amazonaws.services.s3.model PutObjectRequest]
           [org.apache.commons.codec.binary Base64]))


(defn new-client ^AmazonS3Client []
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (AmazonS3Client. credentials)))

(defn set-region [^AmazonS3Client client region]
  (doto client
    (.setRegion (RegionUtils/getRegion region))))

(defn new-transfer-manager ^TransferManager []
  (let [credentials (DefaultAWSCredentialsProviderChain.)] 
    (TransferManager. (AmazonS3Client. credentials))))

(defn upload [^TransferManager transfer-manager ^String bucket ^String key ^bytes serialized ^String content-type 
              encryption ^S3ProgressListener progress-listener]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        encryption-setting (case encryption 
                             :aes256 
                             (ObjectMetadata/AES_256_SERVER_SIDE_ENCRYPTION)
                             :none nil
                             (throw (ex-info "Unsupported encryption type." {:encryption encryption})))
        metadata (ObjectMetadata.)
        _ (.setContentLength metadata size)
        _ (cond-> metadata
            content-type (.setContentMD5 md5))
        _ (cond-> metadata
            encryption-setting (.setSSEAlgorithm encryption-setting))
        put-request (PutObjectRequest. bucket
                                       key
                                       (ByteArrayInputStream. serialized)
                                       metadata)
        upload ^Upload (.upload transfer-manager put-request progress-listener)]
    upload))
