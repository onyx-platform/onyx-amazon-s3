(ns onyx.plugin.s3-multi-output-test
  (:require [clojure.core.async
             :refer
             [<!! >!! alts!! chan close! sliding-buffer timeout]]
            [clojure.test :refer [deftest is testing run-tests]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [add-test-env-peers! feedback-exception! load-config with-test-env]]]
            [onyx.plugin
             [s3-output]
             [core-async :refer [take-segments!]]
             [s3-utils :as s]]
            [onyx.tasks.s3 :as task]
            [taoensso.timbre :as timbre :refer [debug info warn]])
  (:import [com.amazonaws.services.s3.model S3ObjectSummary S3ObjectInputStream]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.services.s3.model ObjectMetadata]))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def serializer-fn (fn [vs]
                     (.getBytes (pr-str (:message vs)) "UTF-8")))

(def deserializer-fn (fn [s]
                       (clojure.edn/read-string (str \( s \)))))

(defn retrieve-s3-results-per-prefix [client bucket prefixes]
  (reduce (fn [acc prefix]
            (assoc acc prefix (s/retrieve-s3-results client bucket deserializer-fn prefix)))
          {}
          prefixes))
; with multiple data groups, each destined for a different s3/prefix, test whether they indeed are sent to the right prefixes
(deftest s3-multi-output-test
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
        client (s/new-client :region "us-east-1")
        bucket (str "s3-plugin-test-" (java.util.UUID/randomUUID))
        _ (.createBucket client bucket)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (let [batch-size 50
              n-messages-per-key 10
              prefixes ["2017/11/01/00" "2017/11/01/01" "2017/11/01/02" "2017/11/01/03" "2017/11/01/04" "2017/11/01/05" "2017/11/01/06" "2017/11/01/07" "2017/11/01/08" "2017/11/01/09" "2017/11/01/10" "2017/11/01/11"]
              n-messages (* n-messages-per-key (count prefixes))
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
                                     :lifecycle/calls ::in-calls}]}
                      (add-task (task/s3-output :out
                                                bucket
                                                ::serializer-fn
                                                {:onyx/max-peers 1
                                                 :s3/max-concurrent-uploads 1
                                                 :s3/encryption :aes256
                                                 :s3/multi-upload true
                                                 :s3/prefix-key :s3/prefix
                                                 :s3/serialize-per-element? true
                                                 :onyx/batch-timeout 2000
                                                 :onyx/batch-size 2000})))
              _ (reset! in-chan (chan (inc n-messages)))
              _ (reset! in-buffer {})
              input-messages (flatten (map (fn [prefix] (map (fn [n] {:s3/prefix prefix
                                                                    :message {:date-hour (str prefix "." n)}})
                                                            (range 0 n-messages-per-key)))
                                           prefixes))]
          (run! #(>!! @in-chan %) input-messages)
          (close! @in-chan)
          (let [job-id (:job-id (onyx.api/submit-job peer-config job))
                _ (feedback-exception! peer-config job-id)
                results (retrieve-s3-results-per-prefix (s/new-client) bucket prefixes)]
            (is (every? #(= n-messages-per-key (count %))  (vals results)))
            (is (= (sort prefixes) (sort (keys results)))))))
      (finally
        (let [ks (s/get-bucket-keys client bucket)]
          (run! (fn [k]
                  (.deleteObject client bucket k))
                ks)
          (.deleteBucket client bucket))))))
