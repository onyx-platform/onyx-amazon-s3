(ns onyx.plugin.s3-utils
  (:import [com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials]
           [com.amazonaws.handlers AsyncHandler]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.event ProgressListener$ExceptionReporter]
           [com.amazonaws.services.s3.transfer TransferManager Upload]
           [com.amazonaws.services.s3 AmazonS3Client AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model S3Object S3ObjectSummary S3ObjectInputStream PutObjectRequest GetObjectRequest ObjectMetadata]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [java.io ByteArrayInputStream InputStreamReader BufferedReader]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.codec.binary Base64]))

(defn new-client ^AmazonS3Client
[& {:keys [access-key secret-key region endpoint-url]}]
  (if-let [builder (cond-> (AmazonS3ClientBuilder/standard)
                           access-key (.withCredentials (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret-key)))
                           region (.withRegion region)
                           endpoint-url (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration. endpoint-url region)))]
    (.build builder)))

(defn transfer-manager ^TransferManager [^AmazonS3Client client]
  (TransferManager. client))

(defn upload [^TransferManager transfer-manager ^String bucket ^String key
              ^bytes serialized ^String content-type encryption]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        encryption-setting (case encryption
                             :aes256
                             (ObjectMetadata/AES_256_SERVER_SIDE_ENCRYPTION)
                             :none nil
                             (throw (ex-info "Unsupported encryption type."
                                             {:encryption encryption})))
        metadata (doto (ObjectMetadata.)
                   (.setContentLength size)
                   (.setContentMD5 md5))
        _ (when content-type
            (.setContentType metadata content-type))
        _ (when encryption-setting
            (.setSSEAlgorithm metadata encryption-setting))
        put-request (PutObjectRequest. bucket
                                       key
                                       (ByteArrayInputStream. serialized)
                                       metadata)
        upload ^Upload (.upload transfer-manager put-request)]
    upload))

(defn upload-synchronous [^AmazonS3Client client ^String bucket ^String k ^bytes serialized]
  (let [size (alength serialized)
        md5 (String. (Base64/encodeBase64 (DigestUtils/md5 serialized)))
        metadata (doto (ObjectMetadata.)
                   (.setContentMD5 md5)
                   (.setContentLength size))]
    (.putObject client
                bucket
                k
                (ByteArrayInputStream. serialized)
                metadata)))

(defn s3-object ^S3Object
[^AmazonS3Client client ^String bucket ^String k & [start-range]]
  (let [object-request (GetObjectRequest. bucket k)
        _ (when start-range
            (.setRange object-request start-range))]
    (.getObject client object-request)))

(defn s3-object ^S3Object
[^AmazonS3Client client ^String bucket ^String k & [start-range]]
  (let [object-request (GetObjectRequest. bucket k)
        _ (when start-range
            (.setRange object-request start-range))]
    (.getObject client object-request)))

(defn list-keys [^AmazonS3Client client ^String bucket ^String prefix]
  (loop [listing (.listObjects client bucket prefix) ks []]
    (let [new-ks (into ks
                       (map (fn [^S3ObjectSummary s] (.getKey s))
                            (.getObjectSummaries listing)))]
      (if (.isTruncated listing)
        (recur (.listObjects client bucket prefix) new-ks)
        new-ks))))
