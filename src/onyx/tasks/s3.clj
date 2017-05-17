(ns onyx.tasks.s3
  (:require [onyx.schema :as os]
            [schema.core :as s]))

;;;;;;;;;;;;;;
;;;;;;;;;;;;;;
;; task schemas

(def Encryption (s/enum :aes256 :none))

(def S3OutputTaskMap
  {:s3/bucket s/Str
   :s3/serializer-fn os/NamespacedKeyword
   :s3/key-naming-fn os/NamespacedKeyword
   (s/optional-key :s3/access-key) s/Str
   (s/optional-key :s3/secret-key) s/Str
   (s/optional-key :s3/serialize-per-element?) s/Bool
   (s/optional-key :s3/serialize-per-element-separator) s/Str
   (s/optional-key :s3/prefix) s/Str
   (s/optional-key :s3/region) s/Str
   (s/optional-key :s3/endpoint-url) s/Str
   (s/optional-key :s3/content-type) s/Str
   (s/optional-key :s3/encryption) Encryption
   (os/restricted-ns :s3) s/Any})

(s/defn ^:always-validate s3-output
  ([task-name :- s/Keyword task-opts :- {s/Any s/Any}]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.s3-output/output
                             :onyx/type :output
                             :onyx/medium :s3
                             :onyx/batch-size 10
                             :s3/encryption :none
                             :s3/key-naming-fn :onyx.plugin.s3-output/default-naming-fn
                             :s3/serialize-per-element-separator "\n"
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

(def S3InputTaskMap 
  {:s3/deserializer-fn os/NamespacedKeyword
   :s3/bucket s/Str
   :s3/prefix s/Str
   (s/optional-key :s3/force-content-encoding) s/Str
   (s/optional-key :s3/buffer-size-bytes) s/Int
   (s/optional-key :s3/region) s/Str
   (s/optional-key :s3/endpoint-url) s/Str
   (s/optional-key :s3/access-key) s/Str
   (s/optional-key :s3/secret-key) s/Str
   (s/optional-key :s3/file-key) s/Str
   (os/restricted-ns :s3) s/Any})

(s/defn ^:always-validate s3-input
  ([task-name :- s/Keyword task-opts :- {s/Any s/Any}]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.s3-input/input
                             :onyx/type :input
                             :onyx/medium :s3
                             :onyx/batch-size 20
                             :onyx/max-peers 1
                             :s3/buffer-size-bytes 10000000
                             :onyx/doc "Reads segments from keys in an S3 bucket."}
                            task-opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.s3-input/s3-input-calls}]}
    :schema {:task-map S3InputTaskMap}})
  ([task-name :- s/Keyword
    bucket :- s/Str
    prefix :- s/Str
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (s3-input task-name (merge {:s3/bucket bucket
                               :s3/prefix prefix
                               :s3/deserializer-fn deserializer-fn}
                              task-opts))))
