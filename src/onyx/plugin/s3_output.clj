(ns onyx.plugin.s3-output
  (:require [onyx
             [extensions :as extensions]
             [schema :as os]
             [types :refer [dec-count! inc-count!]]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer
             [function :as function]
             [operation :refer [kw->fn]]
             [pipeline-extensions :as p-ext]]
            [onyx.plugin.s3-utils :as s3]
            [onyx.tasks.s3 :refer [S3OutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error warn info trace]])
  (:import [com.amazonaws.event ProgressListener]
           [com.amazonaws.event ProgressEventType]
           [com.amazonaws.services.s3.transfer TransferManager Upload]))

(defn results->segments-acks [results]
  (mapcat (fn [{:keys [leaves]} ack]
            (map (fn [leaf]
                   (list (:message leaf)
                         ack))
                 leaves))
          (:tree results)
          (:acks results)))

(defn build-ack-listener [peer-replica-view messenger acks]
  (reify ProgressListener
    (progressChanged [this progressEvent]
      (let [event-type (.getEventType progressEvent)] 
        ;; TODO:
        ;; Fail out? cause peer to reboot?
        ;; (ProgressEventType/TRANSFER_FAILED_REQUEST)
        (cond (= event-type (ProgressEventType/TRANSFER_COMPLETED_EVENT))
              (do
                (trace "s3plugin: progress complete. Acking." event-type)
                (run! (fn [ack] 
                        (when (dec-count! ack)
                          (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                            (extensions/internal-ack-segment messenger site ack))))
                      acks))
              :else
              (trace "s3plugin: progress event." event-type))))))

(defrecord S3Output [serializer-fn transfer-manager bucket]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
    (let [acks (:acks results)
          segments-acks (results->segments-acks results)
          segments (map first segments-acks)]
      (when-not (empty? segments)
        (let [serialized (serializer-fn segments)
              event-listener (build-ack-listener peer-replica-view messenger acks)
              ;; Increment ack reference count because we are writing async
              _ (run! inc-count! (map second segments-acks))]
          (s3/upload transfer-manager bucket (str (java.util.UUID/randomUUID)) serialized event-listener)))
    {}))

  (seal-resource
    [_ event]))

(defn after-task-stop [event lifecycle]
  (.shutdownNow ^TransferManager (:s3/transfer-manager event)))

(defn before-task-start [event lifecycle]
  {:s3/transfer-manager (:transfer-manager (:onyx.core/pipeline event))})

(def s3-output-calls
  {:lifecycle/before-task-start before-task-start
   ;; Implement handle-exception once it's decided in build-ack-listener 
   ;:lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop after-task-stop})

;;; Configurable output pattern - TODO
;; We often want split data according to some pattern e.g.  {bid,click}/ {year}/ {month}/ {day}.json.gz
;; This should be configurable and based on the contents of the message.
;; May need naming function, looked up same as serializer fn
(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate (os/UniqueTaskMap S3OutputTaskMap) task-map)
        {:keys [s3/bucket s3/serializer-fn]} task-map
        transfer-manager (s3/new-transfer-manager)
        serializer-fn (kw->fn serializer-fn)]
    (->S3Output serializer-fn transfer-manager bucket)))
