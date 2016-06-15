(ns onyx.plugin.s3-output-test
  (:require [clojure.core.async
             :refer
             [<!! >!! alts!! chan close! sliding-buffer timeout]]
            [clojure.test :refer [deftest is testing]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [add-test-env-peers! feedback-exception! load-config with-test-env]]]
            [onyx.plugin 
             [s3-output]
             [core-async :refer [take-segments!]]
             [s3-utils :as s]]
            [onyx.tasks.s3 :as task]
            [taoensso.timbre :as timbre :refer [debug info warn]])
  (:import [com.amazonaws.services.s3.model S3ObjectSummary S3ObjectInputStream]))

(def in-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def serializer-fn (fn [vs] 
                     (.getBytes (pr-str vs) "UTF-8")))

(def deserializer-fn (fn [^bytes bs]
                       (read-string (String. bs "UTF-8"))))

(defn read-object [client bucket k]
  (let [object (.getObject client bucket k)
        metadata (.getObjectMetadata object)
        length (.getContentLength metadata)
        bs (byte-array length)
        content ^S3ObjectInputStream (.getObjectContent object)
        read-length (.read content bs)]
    (println read-length)
    (deserializer-fn bs)))

(defn get-bucket-keys [client bucket]
  (map #(.getKey ^S3ObjectSummary %) 
       (.getObjectSummaries (.listObjects
                              client
                              bucket))))

(defn retrieve-s3-results [client bucket]
  (let [ks (get-bucket-keys client bucket)]
    (mapcat (partial read-object client bucket) ks)))

(def ob )

(deftest s3-output-test
  (let [id (java.util.UUID/randomUUID)
        env-config {:onyx/tenancy-id id
                    :zookeeper/address "127.0.0.1:2188"
                    :zookeeper/server? true
                    :zookeeper.server/port 2188}
        peer-config {:onyx/tenancy-id id
                     :zookeeper/address "127.0.0.1:2188"
                     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                     :onyx.messaging.aeron/embedded-driver? true
                     :onyx.messaging/allow-short-circuit? false
                     :onyx.messaging/impl :aeron
                     :onyx.messaging/peer-port 40200
                     :onyx.messaging/bind-addr "localhost"}
        client (s/new-client)
        bucket (str (java.util.UUID/randomUUID))
        _ (.createBucket client bucket)]
    (println "BUCKET " bucket)
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (let [batch-size 2
              job (-> {:workflow [[:in :identity] [:identity :out]]
                       :task-scheduler :onyx.task-scheduler/balanced
                       :catalog [{:onyx/name :in
                                  :onyx/plugin :onyx.plugin.core-async/input
                                  :onyx/type :input
                                  :onyx/medium :core.async
                                  :onyx/batch-size batch-size
                                  :onyx/max-peers 1
                                  :onyx/doc "Reads segments from a core.async channel"}

                                 {:onyx/name :identity
                                  :onyx/fn :clojure.core/identity
                                  :onyx/type :function
                                  :onyx/batch-size batch-size}
                                 ;; Add :out task later
                                 ]
                       :lifecycles [{:lifecycle/task :in
                                     :lifecycle/calls ::in-calls}
                                    {:lifecycle/task :in
                                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}]}
                      (add-task (task/s3-output :out
                                                bucket
                                                ::serializer-fn
                                                {:onyx/max-peers 1})))
              n-messages 100
              _ (reset! in-chan (chan (inc n-messages)))
              input-messages (map (fn [v] {:n v}) (range n-messages))]
          (run! #(>!! @in-chan %) input-messages)
          (println "done")
          (>!! @in-chan :done)
          (println "done done")
          (close! @in-chan)
          (let [job-id (:job-id (onyx.api/submit-job peer-config job))
                _ (feedback-exception! peer-config job-id)
                results (sort-by :n (retrieve-s3-results (s/new-client) "40cfb2d9-7338-4475-a2a7-9228565f49fb"))]
            (is (= results input-messages)))))
      (finally
        (let [ks (get-bucket-keys client bucket)]
          (run! (fn [k]
                  (.deleteObject client bucket k))
                ks)
          (.deleteBucket client bucket))))))
