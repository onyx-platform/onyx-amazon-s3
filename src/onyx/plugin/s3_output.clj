(ns onyx.plugin.s3-output
  (:require [onyx.extensions :as extensions]
            [onyx.schema :as os]
            [onyx.plugin.s3-utils :as s3]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.s3 :refer [S3OutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error warn info trace]]
            [onyx.plugin.protocols :as p])
  (:import [com.amazonaws.event ProgressEventType]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.services.s3.transfer TransferManager Upload Transfer$TransferState]
           [java.io ByteArrayOutputStream]
           [java.util TimeZone]
           [java.text SimpleDateFormat]))

(defn default-naming-fn [event]
  (str (.format (doto (SimpleDateFormat. "yyyy-MM-dd-hh.mm.ss.SSS")
                  (.setTimeZone (TimeZone/getTimeZone "UTC")))
                (java.util.Date.))
       "_batch_"
       (:onyx.core/lifecycle-id event)))

(defn check-failures! [transfers]
  (doseq [[k upload] @transfers]
    (cond (= (Transfer$TransferState/Failed) (.getState ^Upload upload))
          (when-let [e (.waitForException ^Upload upload)]
            (throw e))

          (= (Transfer$TransferState/Completed) (.getState ^Upload upload))
          (swap! transfers dissoc k))))

(defn serialize-per-element [serializer-fn ^String separator elements]
  (let [newline-bs ^bytes (.getBytes separator)] 
    (with-open [baos (ByteArrayOutputStream.)] 
      (run! (fn [element]
              (let [bs ^bytes (serializer-fn element)]
                (.write baos bs 0 (alength bs))
                (.write baos newline-bs 0 (alength newline-bs))))
            elements)
      (.toByteArray baos))))

(defn completed? [transfers]
  (check-failures! transfers)
  (empty? @transfers))

(deftype S3Output [serializer-fn prefix key-naming-fn content-type max-concurrent-uploads
                   encryption ^AmazonS3Client client ^TransferManager transfer-manager 
                   transfers bucket multi-upload prefix-key prefix-separator prepared-batch]
  p/Plugin
  (start [this event]
    this)

  (stop [this event] 
    (.shutdownNow ^TransferManager transfer-manager)
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    (completed? transfers))
  (completed? [this]
    (completed? transfers))
  p/Checkpointed
  (recover! [this replica-version checkpoint]
    (run! #(.abort ^Upload %) (vals @transfers))
    (reset! prepared-batch [])
    (reset! transfers {})
    this)
  (checkpointed! [this epoch])
  (checkpoint [this])

  p/Output
  (prepare-batch [this {:keys [onyx.core/write-batch] :as event} _ _]
    (reset! prepared-batch write-batch)
    true)

  (write-batch [this {:keys [onyx.core/write-batch] :as event} replica _]
    (check-failures! transfers)
    (when-not (empty? @prepared-batch) 
      (let [write-to-prefix-fn (fn [prefix segments]
                                 (let [serialized (serializer-fn segments)
                                       file-name (str prefix prefix-separator (key-naming-fn event))
                                       upload (s3/upload transfer-manager bucket file-name serialized content-type encryption)]
                                   (swap! transfers assoc file-name upload)
                                   upload))] 
        (if multi-upload 
          ;; batch upload
          (let [n-upload-this-batch (min (- max-concurrent-uploads (count @transfers))
                                         (count @prepared-batch))
                to-upload (take n-upload-this-batch @prepared-batch)
                _ (swap! prepared-batch (fn [bt] (drop n-upload-this-batch bt)))]
            (->> to-upload
                 (group-by #(get % prefix-key))
                 (run! (fn [[prefix segments]]
                         (assert prefix "prefix must be given")
                         (write-to-prefix-fn prefix segments)))))
          ;; single upload
          (when (< (count @transfers) max-concurrent-uploads)
            (write-to-prefix-fn prefix @prepared-batch)
            (reset! prepared-batch [])))))

    ;; if we've flushed the batch, we can proceed
    (empty? @prepared-batch)))

(defn after-task-stop [event lifecycle]
  {})

(defn before-task-start [event lifecycle]
  {})

(defn write-handle-exception [event lifecycle lf-kw exception]
  :defer)

(def s3-output-calls
  {:lifecycle/before-task-start before-task-start
   :lifecycle/handle-exception write-handle-exception
   :lifecycle/after-task-stop after-task-stop})

(defn output [{:keys [onyx.core/task-map] :as event}]
  (let [_ (s/validate (os/UniqueTaskMap S3OutputTaskMap) task-map)
        {:keys [s3/bucket s3/serializer-fn s3/key-naming-fn s3/access-key s3/secret-key
                s3/content-type s3/region s3/endpoint-url s3/prefix s3/serialize-per-element? s3/prefix-separator
                s3/transfer-manager-threadpool-size
                s3/multi-upload s3/prefix-key]} task-map
        encryption (or (:s3/encryption task-map) :none)
        max-concurrent-uploads (or (:s3/max-concurrent-uploads task-map) Long/MAX_VALUE)
        prefix-separator (or prefix-separator "/")
        client (s3/new-client :access-key access-key :secret-key secret-key
                              :region region :endpoint-url endpoint-url)
        transfer-manager (s3/transfer-manager client transfer-manager-threadpool-size)
        serializer-fn (kw->fn serializer-fn)
        separator (or (:s3/serialize-per-element-separator task-map) "\n")
        serializer-fn (if serialize-per-element?
                        (fn [segments] (serialize-per-element serializer-fn separator segments))
                        serializer-fn)
        key-naming-fn (kw->fn key-naming-fn)
        transfers (atom {})
        prepared-batch (atom [])]
    (->S3Output serializer-fn prefix key-naming-fn content-type max-concurrent-uploads
                encryption client transfer-manager transfers bucket
                multi-upload prefix-key prefix-separator prepared-batch)))
