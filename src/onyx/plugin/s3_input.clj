(ns onyx.plugin.s3-input
  (:require [onyx.peer.function :as function]
            ;[onyx.peer.pipeline-extensions :as p-ext]
            [clojure.core.async :refer [chan >! >!! <!! close! go thread timeout alts!! poll!  go-loop sliding-buffer]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.schema :as os]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.tasks.s3 :refer [S3InputTaskMap]]
            [onyx.types :as t]
            [onyx.plugin.s3-utils :as s3]
            [onyx.static.util :refer [kw->fn]]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info fatal] :as timbre]) 
  (:import [java.io ByteArrayInputStream InputStreamReader BufferedReader]))

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

;; TODO, shouldn't reboot on validation errors
(defn read-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def s3-input-calls 
  {:lifecycle/before-task-start inject-read-s3-resources
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-s3-resources})

(defprotocol S3Set
  (next-reader [this]) 
  (close-reader [this]))

(deftype S3Input 
  [task-id batch-size batch-timeout deserializer-fn client bucket prefix files
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
            object-input-stream* (s3/s3-object-input-stream client bucket k 0)
            input-stream-reader* (InputStreamReader. object-input-stream*)
            buffered-reader* (BufferedReader. input-stream-reader*)]
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


  i/Input
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

  (synced? [this epoch]
    true)

  (checkpointed! [this epoch]
    true)

  (poll! [this state]
    (if-let [line (and buffered-reader (.readLine ^BufferedReader buffered-reader))]
      (deserializer-fn line)
      (do
       (-> this (close-reader) (next-reader))
       nil)))

  (completed? [this]
    (empty? @files)))

(defn input [{:keys [onyx.core/log onyx.core/task-id onyx.core/task-map] :as event}]
  (let [_ (s/validate (os/UniqueTaskMap S3InputTaskMap) task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        {:keys [s3/bucket s3/prefix s3/deserializer-fn s3/region]} task-map
        client (cond-> (s3/new-client)
                 region (s3/set-region region))
        deserializer-fn (kw->fn deserializer-fn)] 
    (->S3Input task-id batch-size batch-timeout deserializer-fn client 
               bucket prefix (atom nil) (atom nil) nil nil nil nil nil)))
