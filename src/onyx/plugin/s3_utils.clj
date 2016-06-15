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
        upload ^Upload (.upload 
                         transfer-manager
                         bucket
                         key
                         (ByteArrayInputStream. serialized)
                         metadata)]
    (.addProgressListener upload progress-listener)))

; (let [bucket "onyx-s3-plugin-test"
;       key "somestupkey"
;       serialized (byte-array 10)
;       _ (aset serialized 0 (byte \a))]
;   (upload (new-transfer-manager) bucket key serialized)) 

(comment
  byte [] resultByte = DigestUtils.md5 (content);
  String streamMD5 = new String (Base64.encodeBase64 (resultByte));
  metaData.setContentMD5 (streamMD5);


  )

(comment (.getObjectSummaries (.listObjects
                                (new-client "ap-southeast-1")
                                "onyx-s3-plugin-test"
                                )))


;; putObject (PutObjectRequest putObjectRequest)
;; Uploads a new object to the specified Amazon S3 bucket.
;; PutObjectResultputObject (String bucketName, String key, File file)
;; Uploads the specified file to Amazon S3 under the specified bucket and key name.
;; PutObjectResultputObject (String bucketName, String key, InputStream input, ObjectMetadata metadata)
;; Uploads the specified input stream and object metadata to Amazon S3 under the specified bucket and key name.
