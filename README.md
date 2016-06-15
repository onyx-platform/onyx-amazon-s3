## onyx-amazon-s3

Onyx plugin for Amazon S3.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-amazon-s3 "0.9.6.1-SNAPSHOT"]
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
 :s3/serializer-fn :clojure.core/pr-str
 :onyx/type :output
 :onyx/medium :s3
 :onyx/batch-size 20
 :onyx/doc "Writes segments to s3 files, one file per batch"}
```

Segments received at this task will be serialized into a file per batch, named using a function: 
FIXME
FIXME
FIXME
FIXME
FIXME
FIXME
FIXME

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:s3/bucket`                  | `string`  | The name of the s3 bucket to write the file to
|`:s3/serializer-fn`           | `keyword` | A keyword pointing to a fully qualified function that will serialize the batch of segments to bytes

#### Acknowledgments

Many thanks to FIXME FIXME FIXME 

[YourNameHere](http://www.yournamehere.com) for allowing this work to be open
sourced and contributed back to the Onyx Platform community.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
