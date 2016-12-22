(ns onyx.plugin.s3-output
  (:require [onyx.extensions :as extensions]
            [onyx.schema :as os]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.plugin.s3-utils :as s3]
            ; [onyx.log.commands.peer-replica-view :refer [peer-site]]
            ; [onyx.peer
            ;  [function :as function]
            ;  [pipeline-extensions :as p-ext]]
            ;[onyx.static.util :refer [kw->fn]]
            [onyx.tasks.s3 :refer [S3OutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error warn info trace]]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.protocol.task-state :refer [advance get-event]])
  (:import [com.amazonaws.event ProgressEventType]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.services.s3.transfer TransferManager Upload Transfer$TransferState]
           [java.io ByteArrayOutputStream]
           [java.util TimeZone]
           [java.text SimpleDateFormat]))

;;; before starting, add :onyx.core back in to everything

;; kw->fn
;; add input/output/plugin protocols
;; remove types dec-count inc-count
;; remove :onyx/max-pending
;; remove :onyx/pending-timeout
;; move :onyx.core -> :xxx
;; remove test core async reader calls $ ag reader-calls
;; Add test core async buffers

(defn default-naming-fn [event]
  (str (.format (doto (SimpleDateFormat. "yyyy-MM-dd-hh.mm.ss.SSS")
                  (.setTimeZone (TimeZone/getTimeZone "UTC")))
                (java.util.Date.))
       "_batch_"
       (:onyx.core/lifecycle-id event)))

(defn build-ack-listener [fail-fn complete-fn]
  (let [start-time (System/currentTimeMillis)]
    (reify S3ProgressListener
      (progressChanged [this progressEvent]
        (let [event-type (.getEventType progressEvent)] 
          (cond (= event-type (ProgressEventType/CLIENT_REQUEST_FAILED_EVENT))
                (info "Client request failed" event-type)
                (= event-type (ProgressEventType/TRANSFER_FAILED_EVENT))
                (do
                 (info "Transfer failed." event-type)
                 (fail-fn))
                (= event-type (ProgressEventType/TRANSFER_COMPLETED_EVENT))
                (do
                 (complete-fn)
                 (info "s3plugin: progress complete." event-type "took" (- (System/currentTimeMillis) start-time)))
                :else
                (trace "s3plugin: progress event." event-type)))))))

(defn check-failures! [transfers]
  (let [failed-upload (first  
                        (filter (fn [^Upload upload]
                                  (= (Transfer$TransferState/Failed)
                                     (.getState upload)))
                                (vals @transfers)))]
    (when failed-upload
      (when-let [e (.waitForException ^Upload failed-upload)]
        (throw e)))))

(defn serialize-per-element [serializer-fn elements]
  (with-open [baos (ByteArrayOutputStream.)] 
    (run! (fn [element]
            (let [bs ^bytes (serializer-fn element)]
              (.write baos bs 0 (alength bs))))
          elements)
    (.toByteArray baos)))

(deftype S3Output [serializer-fn prefix key-naming-fn content-type 
                   encryption ^AmazonS3Client client ^TransferManager transfer-manager 
                   transfers bucket]
  p/Plugin
  (start [this event]
    this)

  (stop [this event] 
    (.shutdownNow ^TransferManager transfer-manager)
    this)

  o/Output
  (synchronized? [this epoch]
    (check-failures! transfers)
    (empty? @transfers))

  (prepare-batch
    [_ state]
    ;; ADVANCE SHOULD BE NECESSARY IN PREPARE
    state)

  (write-batch [_ state]
    (let [{:keys [onyx.core/results] :as event} (get-event state)
          ;; TODO: need to get rid of leafs
          segments (map :message (mapcat :leaves (:tree results)))]
      (check-failures! transfers)
      (when-not (empty? segments)
        (let [serialized (serializer-fn segments)
              file-name (str prefix (key-naming-fn event))
              fail-fn (fn [] (swap! transfers dissoc file-name))
              complete-fn (fn [] (swap! transfers dissoc file-name))
              event-listener (build-ack-listener fail-fn complete-fn)
              upload (s3/upload transfer-manager bucket file-name serialized
                                content-type encryption event-listener)]
          (swap! transfers assoc file-name upload) ))
      (advance state))))

(defn after-task-stop [event lifecycle]
  {})

(defn before-task-start [event lifecycle]
  {})

;; TODO, shouldn't reboot on validation errors
(defn write-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def s3-output-calls
  {:lifecycle/before-task-start before-task-start
   :lifecycle/handle-exception write-handle-exception
   :lifecycle/after-task-stop after-task-stop})

(defn output [{:keys [onyx.core/task-map] :as event}]
  (let [_ (s/validate (os/UniqueTaskMap S3OutputTaskMap) task-map)
        {:keys [s3/bucket s3/serializer-fn s3/key-naming-fn 
                s3/content-type s3/endpoint s3/region s3/prefix s3/serialize-per-element?]} task-map
        encryption (or (:s3/encryption task-map) :none)
        _ (when (and region endpoint)
            (throw (ex-info "Cannot use both :s3/region and :s3/endpoint with the S3 output plugin."
                            task-map)))
        ;; FIXME DOC REGION ENDPOINT
        client (cond-> (s3/new-client)
                 endpoint (s3/set-endpoint endpoint)
                 region (s3/set-region region))
        transfer-manager (s3/transfer-manager client)
        transfers (atom {})
        serializer-fn (kw->fn serializer-fn)
        serializer-fn (if serialize-per-element? 
                        (fn [segments] (serialize-per-element serializer-fn segments))
                        serializer-fn)
        key-naming-fn (kw->fn key-naming-fn)]
    (->S3Output serializer-fn prefix key-naming-fn content-type 
                encryption client transfer-manager transfers bucket)))
