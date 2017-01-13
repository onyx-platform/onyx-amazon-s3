(ns onyx.plugin.s3-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [clojure.core.async :refer [chan >! >!! <!! close! go thread timeout alts!! poll!  go-loop sliding-buffer]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.tasks.s3 :refer [S3InputTaskMap]]
            [onyx.types :as t]
            [onyx.plugin.s3-utils :as s3]
            [onyx.static.util :refer [kw->fn]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info fatal] :as timbre]) 
  (:import [java.io ByteArrayInputStream InputStreamReader BufferedReader]))

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defn close-readers! [readers]
  (when-let [{:keys [input-stream input-stream-reader buffered-reader]} @readers]
    (.close ^BufferedReader buffered-reader)
    (.close ^InputStreamReader input-stream-reader)
    (.close ^com.amazonaws.services.s3.model.S3ObjectInputStream input-stream))
  (reset! readers nil))

(defn close-read-s3-resources 
  [{:keys [onyx.core/pipeline] :as event} lifecycle]
  (while (poll! (:retry-ch pipeline)))
  (close! (:retry-ch pipeline))
  (close-readers! (:readers pipeline))
  {})

(defn update-min-indices [files]
  (reduce (fn [fs [k {:keys [pending-indices fully-read?]}]]
            (if (and (empty? pending-indices)
                     fully-read?)
              fs
              (assoc-in fs 
                        [k :max-acked-index] 
                        (if (empty? pending-indices) 
                          -1
                          (apply min pending-indices)))) )
          files
          files))

(defn strip-pending-indices [files]
  (reduce (fn [fs k]
            (update fs k dissoc :pending-indices))
          files
          (keys files)))

(defn start-commit-loop! [event log files checkpoint-key]
  (thread
   (try
    (loop []
      (when-not (first (alts!! [(:onyx.core/task-kill-ch event) (:onyx.core/kill-ch event)] :default true))
        (let [content (-> files 
                          (swap! update-min-indices)
                          strip-pending-indices)]
          (extensions/force-write-chunk log :chunk content checkpoint-key))
        (Thread/sleep 1000)
        (recur)))
    (catch Throwable t
      (info "onyx-amazon-s3, input plugin commit loop failed." t)))))

(defn init-pending-indices [files]
  (reduce (fn [fs k]
            (assoc-in fs [k :pending-indices] #{}))
          files
          (keys files)))

(defn inject-read-seq-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/job-id 
           onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "s3 input tasks must currently use :onyx/max-peers 1" task-map)))
  (let [files (:files pipeline)
        checkpoint-key (str job-id "#" task-id)
        ;; we don't even need to do the initial s3 scan
        _ (extensions/write-chunk log :chunk @files checkpoint-key)
        content (extensions/read-chunk log :chunk checkpoint-key)]
    (reset! files (init-pending-indices content))
    (let [commit-loop-ch (start-commit-loop! event log files checkpoint-key)]
      {:s3/drained? (:drained pipeline)
       :s3/pending-messages (:pending-messages pipeline)})))

(defn read-handle-exception [event lifecycle lf-kw exception]
  :defer)

(def s3-input-calls 
  {:lifecycle/before-task-start inject-read-seq-resources
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-s3-resources})

(defn take-values! [batch ch n-messages]
  (loop [n 0]
    (if-let [v (if (< n n-messages)
                   (poll! ch))]
      (do (conj! batch v)
          (recur (inc n)))
      n)))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defn completed? [files batch pending-messages retry-ch]
  (and (all-done? (vals @pending-messages))
       (all-done? batch)
       (empty? (remove (fn [[k v]]
                         (:fully-read? v)) 
                       @files))
       (zero? (count (.buf retry-ch)))))

(defn next-reader! [client buffer-size bucket readers files]
  (if (nil? @readers)
    (if-let [f (->> @files
                    (remove (fn [[k v]]
                              (:fully-read? v)))
                    (sort-by (comp :top-index val))
                    last)]
      (let [k (key f)
            object (s3/s3-object client bucket k 0)
            object-input-stream (.getObjectContent object)
            object-length (.getContentLength (.getObjectMetadata object))
            input-stream-reader (InputStreamReader. object-input-stream)
            buffered-reader (BufferedReader. input-stream-reader (or buffer-size object-length))]
        (dotimes [line (:top-index (val f))]
          ;; skip over fully acked segments
          (.readLine buffered-reader))
        (reset! readers {:k k
                         :input-stream object-input-stream
                         :input-stream-reader input-stream-reader
                         :buffered-reader buffered-reader}))
      (reset! readers nil)))
  @readers)

(defrecord S3Input 
  [log task-id max-pending batch-size batch-timeout buffer-size pending-messages drained? 
   top-line-index top-acked-line-index pending-line-indices commit-ch retry-ch
   deserializer-fn client bucket files readers]
  p-ext/Pipeline
  (write-batch [this event]
    (function/write-batch event))

  (read-batch 
    [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          tbatch (transient [])
          _ (take-values! tbatch retry-ch max-segments)
          _ (if-let [{:keys [k buffered-reader line-number]} (next-reader! client buffer-size bucket readers files)] 
              (loop []
                (when (<= (count tbatch) max-segments)
                  (if-let [line (.readLine ^BufferedReader buffered-reader)]
                    (do 
                     (conj! tbatch 
                            (-> (t/input (random-uuid) (deserializer-fn line))
                                (assoc :k k)
                                (assoc :line-number (get-in (swap! files update-in [k :top-index] inc) [k :top-index]))))  
                     (recur))
                    (do
                     (swap! files assoc-in [k :fully-read?] true)
                     (close-readers! readers)))))
              (when-not @drained? 
                (conj! tbatch (t/input (random-uuid) :done))))
          batch (persistent! tbatch)]
      (doseq [m batch]
        (when-not (= :done (:message m)) 
          (swap! files update-in [(:k m) :pending-indices] conj (:line-number m)))
        (swap! pending-messages assoc (:id m) m)) 
      (when (completed? files batch pending-messages retry-ch)
        (reset! drained? true))
      {:onyx.core/batch batch}))

  (seal-resource [this event])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (let [m (@pending-messages segment-id)]
      (swap! files update-in [(:k m) :pending-indices] disj (:line-number m))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment 
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id) 
      (>!! retry-ch (assoc msg :id (random-uuid)))))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ _]
    @drained?))

(defn input [{:keys [onyx.core/log onyx.core/task-id 
                     onyx.core/task-map] :as event}]
  (let [_ (s/validate (os/UniqueTaskMap S3InputTaskMap) task-map)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        {:keys [s3/bucket s3/prefix s3/deserializer-fn s3/access-key 
                s3/secret-key s3/region s3/buffer-size-bytes]} task-map
        pending-messages (atom {})
        drained? (atom false)
        top-line-index (atom -1)
        top-acked-line-index (atom -1)
        pending-line-indices (atom #{})
        retry-ch (chan 1000000)
        commit-ch (chan (sliding-buffer 1))
        client (cond-> (if access-key 
                         (s3/new-client access-key secret-key)
                         (s3/new-client))
                 region (s3/set-region region))
        deserializer-fn (kw->fn deserializer-fn)
        files (->> (s3/list-keys client bucket prefix)
                   (map (fn [file]
                          {file {:fully-read? false
                                 :top-index -1
                                 :max-acked-index -1}}))
                   (into {}))] 
    (->S3Input log task-id max-pending batch-size batch-timeout buffer-size-bytes pending-messages drained? 
               top-line-index top-acked-line-index pending-line-indices commit-ch retry-ch
               deserializer-fn client bucket (atom files) (atom nil))))
