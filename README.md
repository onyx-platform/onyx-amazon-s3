## onyx-amazon-s3

Onyx plugin for Amazon S3.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-amazon-s3 "0.9.13.1-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.s3-output])
```

#### Functions

##### Output Task

Catalog entry:

```clojure
{:onyx/name <<TASK_NAME>>
 :onyx/plugin :onyx.plugin.s3-output/output
 :s3/bucket <<BUCKET_NAME>>
 :s3/encryption :none
 :s3/serializer-fn :clojure.core/pr-str
 :s3/key-naming-fn :onyx.plugin.s3-output/default-naming-fn
 :onyx/type :output
 :onyx/medium :s3
 :onyx/batch-size 20
 :onyx/doc "Writes segments to s3 files, one file per batch"}
```

Segments received by this task will be serialized by the `:s3/serializer-fn`,
into a file per batch, placed at a key in the bucket which is named via the
function defined at `:s3/key-naming-fn`. This function takes an event map and
returns a string. Using the default naming function, `:onyx.plugin.s3-output/default-naming-fn`,
 will name keys in the following format in UTC time format:
 "yyyy-MM-dd-hh.mm.ss.SSS_batch_BATCH_UUID".
You can define `:s3/encryption` to be `:aes256` if your S3 bucket has encryption enabled. The default value is `:none`.

Lifecycle entry:

```clojure
{:lifecycle/task <<TASK_NAME>>
 :lifecycle/calls :onyx.plugin.s3-output/s3-output-calls}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:s3/bucket`                  | `string`  | The name of the s3 bucket to write the file to
|`:s3/serializer-fn`           | `keyword` | A keyword pointing to a fully qualified function that will serialize the batch of segments to bytes
|`:s3/key-naming-fn`           | `keyword` | A keyword pointing to a fully qualified function that be supplied with the Onyx event map, and produce an s3 key for the batch.  
|`:s3/content-type`            | `string`  | Optional content type for value
|`:s3/encryption`              | `keyword` | Optional encryption setting. One of `:sse256` or `:none`.

#### Acknowledgments

Many thanks to [AdGoji](http://www.adgoji.com) for allowing this work to be open sourced and contributed back to the Onyx Platform community.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
