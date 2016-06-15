(ns onyx.tasks.s3
  (:require [onyx.schema :as os]
            [schema.core :as s]))

;;;;;;;;;;;;;;
;;;;;;;;;;;;;;
;; task schemas

(def S3OutputTaskMap
  {:s3/bucket s/Str
   :s3/serializer-fn os/NamespacedKeyword
   (os/restricted-ns :s3) s/Any})

(s/defn ^:always-validate s3-output
  ([task-name :- s/Keyword task-opts :- {s/Any s/Any}]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.s3-output/output
                             :onyx/type :output
                             :onyx/medium :s3
                             :onyx/batch-size 10
                             :onyx/doc "Writes segments to files in an S3 bucket."}
                            task-opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.s3-output/s3-output-calls}]}
    :schema {:task-map S3OutputTaskMap}})
  ([task-name :- s/Keyword
    bucket :- s/Str
    serializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (s3-output task-name (merge {:s3/bucket bucket
                                :s3/serializer-fn serializer-fn}
                               task-opts))))
