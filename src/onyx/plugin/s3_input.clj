(ns onyx.plugin.s3-input
  (:require [clojure.core.async :refer [chan >! >!! <!! close! go thread timeout alts!! poll!  go-loop sliding-buffer]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.tasks.s3 :refer [S3InputTaskMap]]
            [onyx.types :as t]
            [onyx.plugin.s3-utils :as s3]
            [onyx.static.util :refer [kw->fn]]
            [onyx.plugin.protocols :as p]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info fatal] :as timbre])
  (:import [java.io ByteArrayInputStream InputStreamReader BufferedReader]
           [java.util.concurrent.locks LockSupport]))

(defn close-readers! [readers]
  (when-let [{:keys [input-stream input-stream-reader buffered-reader]} @readers]
    (.close ^BufferedReader buffered-reader)
    (.close ^InputStreamReader input-stream-reader)
    (.close ^com.amazonaws.services.s3.model.S3ObjectInputStream input-stream))
  (reset! readers nil))

(defn close-read-s3-resources 
  [event lifecycle]
  {})

(defn inject-read-s3-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/job-id onyx.core/task-id] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "s3 input tasks must currently use :onyx/max-peers 1" task-map)))
  {})

(defn read-handle-exception [event lifecycle lf-kw exception]
  :defer)

(def s3-input-calls 
  {:lifecycle/before-task-start inject-read-s3-resources
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-s3-resources})

(defprotocol S3Set
  (next-reader [this]) 
  (close-reader [this]))

(deftype S3Input 
    [task-id batch-size batch-timeout content-encoding buffer-size-bytes
     deserializer-fn extraction-fn client bucket prefix files
     readers ^:unsynchronized-mutable s3-key ^:unsynchronized-mutable input-stream
     ^:unsynchronized-mutable input-stream-reader ^:unsynchronized-mutable buffered-reader
     ^:unsynchronized-mutable segment]

  p/Plugin
  (start [this event]
    this)

  (stop [this event] 
    (close-reader this))

  S3Set
  (next-reader [this]
    (when-let [f (->> @files (sort-by val) last)]
      (let [k (key f)
            object (s3/s3-object client bucket k 0)
            object-input-stream* (.getObjectContent object)
            object-metadata (.getObjectMetadata object)
            content-encoding (or content-encoding (.getContentEncoding object-metadata))
            object-length (.getContentLength object-metadata)
            input-stream-reader* (if content-encoding 
                                   (InputStreamReader. object-input-stream* ^String content-encoding)
                                   (InputStreamReader. object-input-stream*))
            buffered-reader* (BufferedReader. input-stream-reader* (min buffer-size-bytes object-length))]
        (dotimes [line (val f)]
          ;; skip over fully acked segments
          (.readLine buffered-reader*))
        (set! s3-key k)
        (set! buffered-reader buffered-reader*)
        (set! input-stream-reader input-stream-reader*)
        (set! input-stream object-input-stream*)))
    this)

  (close-reader [this]
    (when s3-key
      (.close ^BufferedReader buffered-reader)
      (.close ^InputStreamReader input-stream-reader)
      (.close ^com.amazonaws.services.s3.model.S3ObjectInputStream input-stream)
      (swap! files dissoc s3-key))
    (set! s3-key nil)
    (set! buffered-reader nil)
    (set! input-stream-reader nil)
    (set! input-stream nil)
    this)

  p/Checkpointed
  (checkpoint [this]
    @files)
  (recover! [this replica-version checkpoint]
    (reset! files 
            (if (or (nil? checkpoint) (= checkpoint :beginning)) 
              (->> (s3/list-keys client bucket prefix)
                   (map (fn [file] {file -1}))
                   (into {}))
              checkpoint))
    this)
  (checkpointed! [this epoch]
    true)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this]
    (empty? @files))

  p/Input
  (poll! [this state]
    (if-let [line (and buffered-reader (.readLine ^BufferedReader buffered-reader))]
      (extraction-fn (deserializer-fn line) {:s3-key s3-key})
      (do
        (-> this (close-reader) (next-reader))
        nil))))

(defn build-extraction-fn [file-key]
  (if file-key
    (fn [m {:keys [s3-key]}] (assoc m file-key s3-key))
    (fn [m more] m)))

(defn input [{:keys [onyx.core/log onyx.core/task-id onyx.core/task-map] :as event}]
  (let [_ (s/validate (os/UniqueTaskMap S3InputTaskMap) task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        {:keys [s3/bucket s3/prefix s3/deserializer-fn s3/region s3/endpoint-url
                s3/buffer-size-bytes s3/content-encoding s3/access-key
                s3/secret-key s3/file-key]} task-map
        client (s3/new-client :access-key access-key :secret-key secret-key
                              :region region :endpoint-url endpoint-url)
        deserializer-fn (kw->fn deserializer-fn)
        extraction-fn (build-extraction-fn file-key)]
    (->S3Input task-id batch-size batch-timeout content-encoding buffer-size-bytes deserializer-fn
               extraction-fn client bucket prefix (atom nil) (atom nil) nil nil nil nil nil)))
