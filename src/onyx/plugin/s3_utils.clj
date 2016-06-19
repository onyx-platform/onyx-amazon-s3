(ns onyx.plugin.s3-utils
  (:import [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.handlers AsyncHandler]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.services.s3.model ObjectMetadata]
           [java.io ByteArrayInputStream]
           [com.amazonaws.services.s3.transfer TransferManager Upload]
           [com.amazonaws.event ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.codec.binary Base64]
           ;; Just for testing
           [java.nio.file Files FileSystems]))


(defn new-client ^AmazonS3Client []
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (AmazonS3Client. credentials)))

(defn set-region [^AmazonS3Client client region]
  (doto client
    (.setRegion (RegionUtils/getRegion region))))

(defn new-transfer-manager ^TransferManager []
  (let [credentials (DefaultAWSCredentialsProviderChain.)] 
    (TransferManager. (AmazonS3Client. credentials))))

(defn upload [^TransferManager transfer-manager ^String bucket ^String key ^bytes serialized ^ProgressListener progress-listener]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        metadata (doto (ObjectMetadata.)
                   (.setContentLength size)
                   ; (.setContentType contentType)
                   (.setContentMD5 md5))
        upload ^Upload (.upload transfer-manager
                                bucket
                                key
                                (ByteArrayInputStream. serialized)
                                metadata)]
    (.addProgressListener upload progress-listener)))
