# clojure-bigtable

## BigTable summary

BigTable is a key-value store. Unlike Redis, it is backed by durable storage (disks), cheaper per GB, and stores keys in lexicographical order, which unlocks additional access patterns.

Each table in BigTable is a series of *rows* (identified by a single bytestring key). Each row has values for one or more *columns*. Columns belong to *column families*, which are sets of semantically-related columns. A *cell* is the value of a single row at a particular column. For each cell, a timeseries of past values for the cell is stored, with garbage collection policies to periodically clean up old values.

For [simplicity](https://github.com/RakutenReady/clojure-bigtable/pull/1#issuecomment-726970948), this library doesn't use the timeseries feature of cells. Instead, each cell value's timestamp is explicitly set to zero when writing data. If this library is used to read data from a table that wasn't created by this library, it always returns the newest value for each cell.

## Setup

By default, this library will use your local GCP credentials and you can access
real BigTable instances.

To run tests or learn about BigTable, use the [emulator](https://cloud.google.com/bigtable/docs/emulator).

## Usage

```clojure
(require '[clojure-bigtable.core :as core] :reload)
(require '[clojure-bigtable.admin :as admin] :reload)

(def client (core/create-client "prediction-s1-efad" "test-instance-id"))
(def admin-client (admin/create-admin-client "prediction-s1-efad" "test-instance-id"))

;; Create test-table with a single column family, "cf1"
(admin/create-table admin-client "test-table" ["cf1"])

;; Check if row exists in table. Throws if table DNE.
(deref (core/exists-async client "test-table" (byte-array [1 2 3])))
;; => false

;; Set the value of a row.
(deref (core/set-row-async
         client
         "test-table"
         (byte-array [1 2 3])
         ;; The row contents to add. Each key is a column family and column.
         ;; Each value is a byte array.
         {["column_family_1" "column_1"] (byte-array [ 4 5 6])}))
;; => nil

;; Read row. Throws if table DNE.
(deref (core/read-row-async client "test-table" (byte-array [1 2 3])))
;; => {["column_family_1" "column_1"] (byte-array [ 4 5 6])}
```
