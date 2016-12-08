(ns onyx.plugin.s3-input-test
  (:require [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.tasks [core-async :as ca]]
            [onyx.plugin
             [s3-input]
             [core-async :refer [take-segments! get-core-async-channels]]]))

(defn build-job [bucket batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :out]]
                  :catalog [{:onyx/name :in
                             :onyx/plugin :onyx.plugin.s3-input/input
                             :onyx/type :input
                             :onyx/max-pending 500
                             :onyx/medium :s3
                             :onyx/batch-size 1
                             :onyx/max-peers 1
                             :onyx/doc "Reads segments from a core.async channel"}]
                  :lifecycles [{:lifecycle/task :in
                                :lifecycle/calls :onyx.plugin.s3-input/s3-input-calls}]
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        ;(add-task (seq/buffered-file-reader :in "test-resources/lines.txt" batch-settings))
        (add-task (ca/output :out batch-settings)))))

(deftest s3-input-test
  (let [id (java.util.UUID/randomUUID)
        env-config {:onyx/tenancy-id id
                    :zookeeper/address "127.0.0.1:2188"
                    :zookeeper/server? true
                    :zookeeper.server/port 2188}
        peer-config {:onyx/tenancy-id id
                     :zookeeper/address "127.0.0.1:2188"
                     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                     :onyx.log/config {:appenders
                                       {:println
                                        {:min-level :trace
                                         :enabled? true}}}
                     :onyx.messaging.aeron/embedded-driver? true
                     :onyx.messaging/allow-short-circuit? false
                     :onyx.messaging/impl :aeron
                     :onyx.messaging/peer-port 40200
                     :onyx.messaging/bind-addr "localhost"}
        job (build-job "XXXBUCKET" 10 1000)
        {:keys [out]} (get-core-async-channels job)]
    (with-test-env [test-env [2 env-config peer-config]]
      (onyx.test-helper/validate-enough-peers! test-env job)
      (onyx.api/submit-job peer-config job)
      (println "submitted job")
      (is (= (set (butlast (take-segments! out)))
             #{{:val "line1"} {:val "line2"} {:val "line3"}
               {:val "line4"} {:val "line5"} {:val "line6"}
               {:val "line7"} {:val "line8"} {:val "line9"}})))))
