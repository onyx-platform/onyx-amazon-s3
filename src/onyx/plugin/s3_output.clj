(ns onyx.plugin.s3-output
  (:require [onyx
             [extensions :as extensions]
             [schema :as os]
             [types :refer [dec-count! inc-count!]]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer
             [function :as function]
             [pipeline-extensions :as p-ext]]
            [onyx.plugin.s3-utils :as s3]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.s3 :refer [S3OutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error warn info trace]])
  (:import [com.amazonaws.event ProgressEventType]
           [com.amazonaws.services.s3.transfer.internal S3ProgressListener]
           [com.amazonaws.services.s3.transfer TransferManager Upload Transfer$TransferState]
           [java.util TimeZone]
           [java.text SimpleDateFormat]))

(defn results->segments-acks [results]
  (mapcat (fn [{:keys [leaves]} ack]
            (map (fn [leaf]
                   (list (:message leaf)
                         ack))
                 leaves))
          (:tree results)
          (:acks results)))

(defn default-naming-fn [event]
  (str (.format (doto (SimpleDateFormat. "yyyy-MM-dd-hh.mm.ss.SSS")
                  (.setTimeZone (TimeZone/getTimeZone "UTC")))
                (java.util.Date.))
       "_batch_"
       (:onyx.core/lifecycle-id event)))


(defn build-ack-listener [fail-fn complete-fn peer-replica-view messenger acks]
  (let [start-time (System/currentTimeMillis)]
    (reify S3ProgressListener
      (progressChanged [this progressEvent]
        (let [event-type (.getEventType progressEvent)] 
          ;; TODO:
          (cond (= event-type (ProgressEventType/CLIENT_REQUEST_FAILED_EVENT))
                (info "Client request failed" event-type)
                (= event-type (ProgressEventType/TRANSFER_FAILED_EVENT))
                (do
                 (info "Transfer failed." event-type)
                 (fail-fn))
                (= event-type (ProgressEventType/TRANSFER_COMPLETED_EVENT))
                (do
                 (complete-fn)
                 (info "s3plugin: progress complete. Acking." event-type "took" (- (System/currentTimeMillis) start-time))
                 (run! (fn [ack] 
                         (when (dec-count! ack)
                           (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                             (extensions/internal-ack-segment messenger site ack))))
                       acks))
                :else
                (trace "s3plugin: progress event." event-type)))))))

(defn check-failures! [transfers]
  (let [failed-uploads (filter (fn [upload]
                                 (= (Transfer$TransferState/Failed)
                                    (.getState upload)))
                               (vals @transfers))]
    (when-not (empty? failed-uploads)
      (when-let [e (.waitForException (first failed-uploads))]
        (throw e)))))

(defrecord S3Output [serializer-fn key-naming-fn content-type transfer-manager transfers bucket]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
    (let [acks (:acks results)
          segments-acks (results->segments-acks results)
          segments (map first segments-acks)]
      (check-failures! transfers)
      (when-not (empty? segments)
        (let [serialized (serializer-fn segments)
              file-name (key-naming-fn event)
              fail-fn (fn [] (swap! transfers dissoc file-name))
              complete-fn (fn [] (swap! transfers dissoc file-name))
              event-listener (build-ack-listener fail-fn complete-fn peer-replica-view messenger acks)
              ;; Increment ack reference count because we are writing async
              _ (run! inc-count! (map second segments-acks))
              upload (s3/upload transfer-manager bucket file-name serialized content-type event-listener)]
              (swap! transfers assoc file-name upload)))
    {}))

  (seal-resource
    [_ event]))

(defn after-task-stop [event lifecycle]
  (.shutdownNow ^TransferManager (:s3/transfer-manager event)))

(defn before-task-start [event lifecycle]
  {:s3/transfer-manager (:transfer-manager (:onyx.core/pipeline event))})

(defn read-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def s3-output-calls
  {:lifecycle/before-task-start before-task-start
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop after-task-stop})

(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate (os/UniqueTaskMap S3OutputTaskMap) task-map)
        {:keys [s3/bucket s3/serializer-fn s3/key-naming-fn s3/content-type]} task-map
        transfer-manager (s3/new-transfer-manager)
        transfers (atom {})
        serializer-fn (kw->fn serializer-fn)
        key-naming-fn (kw->fn key-naming-fn)]
    (->S3Output serializer-fn key-naming-fn content-type transfer-manager transfers bucket)))
