(ns onyx.s3.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.s3-input/input
    {:summary "An input task to read keys from an 33 bucket."
     :model {:s3/deserializer-fn 
             {:doc ""
              :type :keyword}

             :s3/bucket 
             {:doc ""
              :type :string}

             :s3/prefix 
             {:doc ""
              :type :string}}}

    :onyx.plugin.s3-output/output
    {:summary "An output task to write messages to an S3 bucket."
     :model {:s3/bucket 
             {:doc ""
              :type :string}

             :s3/prefix 
             {:doc ""
              :optional? true
              :type :string}

             :s3/serializer-fn 
             {:doc ""
              :type :keyword}

             :s3/key-naming-fn 
             {:doc ""
              :type :keyword}


             :s3/serialize-per-element? 
             {:doc ""
              :optional? true
              :type :boolean}

             :s3/endpoint 
             {:doc ""
              :optional? true
              :type :string}

             :s3/region 
             {:doc ""
              :optional? true
              :type :string}


             :s3/content-type 
             {:doc ""
              :optional? true
              :type :string}

             :s3/encryption 
             {:doc ""
              :choices [:none :aes256]
              :optional? true
              :type :keyword}}}}

   :lifecycle-entry
   {:onyx.plugin.s3-input/input
    {:model
     [{:task.lifecycle/task :input
       :lifecycle/calls :onyx.plugin.s3-input/s3-input-calls}]}

    :onyx.plugin.s3-output/output
    {:model
     [{:task.lifecycle/task :output
       :lifecycle/calls :onyx.plugin.s3-output/s3-output-calls}]}}

   :display-order
   {:onyx.plugin.s3-input/input
    [:s3/deserializer-fn :s3/bucket :s3/prefix]

    :onyx.plugin.s3-output/output
    [:s3/serializer-fn :s3/prefix :s3/region :s3/encryption :s3/content-type :s3/serialize-per-element? :s3/key-naming-fn :s3/endpoint :s3/bucket]}})
