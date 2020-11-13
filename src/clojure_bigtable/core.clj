(ns clojure-bigtable.core
  "Core API to BigTable.

   A table is a collection of rows lexicographically ordered by key.

   Each row consists of a set of cells, which contain a timeseries of bytestring
   values. Each cell is uniquely identified by a column family and column.

   To represent a row in Clojure, we use a map where each key is of the form
   [column-family-id column-id] and each value is a byte string.

   The timestamp portion of the BigTable API is ignored by this library. Instead,
   we use Google's recommended best practices for only allowing one value per
   cell: https://cloud.google.com/bigtable/docs/gc-latest-value. This decision
   simplifies the API, reduces our chances of making costly mistakes, and
   reduces implementation effort. See
   https://github.com/RakutenReady/clojure-bigtable/pull/1#issuecomment-726970948
   for more information."
  (:require
   [clojure.core.async :as async]
   [medley.core :as medley])
  (:import
   (com.google.api.gax.rpc ResponseObserver)
   (com.google.cloud.bigtable.data.v2 BigtableDataClient)
   (com.google.cloud.bigtable.data.v2.models BulkMutation Mutation Query RowCell RowMutation)
   (com.google.protobuf ByteString ByteString$LiteralByteString)
   (java.nio.charset StandardCharsets)))

(defn- maybe-coerce-to-byte-string
  "Coerce to bytestring if possible. Otherwise, return nil."
  [x]
  (cond
    (string? x)
    (ByteString/copyFrom (.getBytes x StandardCharsets/UTF_8))
    (bytes? x)
    (ByteString/copyFrom x)
    :else nil))

(defn- coerce-to-byte-string
  "Coerce to a bytestring if possible. Otherwise, throw an exception."
  [x]
  {:post [(= (type %) ByteString$LiteralByteString)]}
  (if-let [result (maybe-coerce-to-byte-string x)]
    result
    (throw (Exception. (str "Can't convert " (type x) " to bytes.")))))

(defn create-client
  "Creates a client that can perform BigTable operations. Internally, a
   client is a gRPC client connection to Google Cloud. Clients:
   1. Are slow to create. Reuse this throughout your application.
   2. Are thread safe except for Batchers (not yet implemented in this lib)
   3. MUST be destroyed with a call to .close() or destroy-client"
  [project-id instance-id]
  (BigtableDataClient/create project-id instance-id))

(defn destroy-client
  "Closes the underlying connections in the given client. The client cannot be
   used afterwards. Must be called when you are done with the client.

   If any asynchronous BigTable operations are in-flight when this is called,
   you might see strange results."
  [^BigtableDataClient client]
  (.close client))

(defn exists-async
  "Returns a future that resolves to true iff the given row key exists."
  [^BigtableDataClient client ^String table-id ^bytes row-key]
  {:pre [client (string? table-id)]}
  (.existsAsync client table-id (coerce-to-byte-string row-key)))

(defn- Row->map
  "Converts a Row to a map."
  [x]
  (->> x
      (.getCells)
      (into [])
      (map (fn [cell]
             [[(.getFamily cell) (.toStringUtf8 (.getQualifier cell))]
              {:value (into [] (.toByteArray (.getValue cell)))
               :timestamp (.getTimestamp cell)}]))
       (group-by first)
       (medley/map-vals #(map second %))
       ;; TODO: we are throwing away older values of cells on this step.
       ;; This is bad for two reasons:
       ;; 1. the user might want older cell values
       ;; 2. silently throwing away data can mask performance problems --
       ;;    if we have a huge number of old values for a cell, we are wasting
       ;;    bandwidth fetching it from Bigtable and then just discarding it.
       ;;
       ;; However, we currently don't need historical values. Improve the API
       ;; to allow access to historical values later.
       (medley/map-vals  #(apply max-key :timestamp %))
       (medley/map-vals :value)))

(defn read-row-async
  "Returns a future that resolves to the contents of a single row, as a map.
   Resolves to nil if the row does not exist. The newest value of each cell is
   returned."
  [^BigtableDataClient client ^String table-id ^bytes row-key]
  {:pre [client (string? table-id)]}
  (future
    (let [row-future (.readRowAsync client table-id (coerce-to-byte-string row-key))
          row-result (deref row-future)]
      (Row->map row-result))))

(defn set-row-cell-async
  "Set the contents of one cell of a row. Overwrites the value if it already
   exists. value can be either string, bytes, or a long.

   If you need to set multiple cells in a row, this functions is very
   inefficient. Use set-row-async instead.

   Returns a future that returns nil."
  [^BigtableDataClient client
   ^String table-id
   ^bytes row-key
   ^String family-id
   ^String column-id
   value]
  {:pre [client (string? table-id) (string? family-id) (string? column-id)]}
  (let [row-mutation
        (-> (RowMutation/create table-id
                                (coerce-to-byte-string row-key))
            (.setCell family-id
                      (coerce-to-byte-string column-id)
                      0 ;; timestamp
                      (or (maybe-coerce-to-byte-string value) value)))]
    (.mutateRowAsync client row-mutation)))

(defn- set-cells-from-row-map
  "For any object that implements the MutationApi
   https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/models/MutationApi.html
   , populate the object's set of mutations using the provided specification
   of a row in the row-map format."
  [mutation-api-obj row-map]
  (doseq [[[family-id column-id] value] row-map]
    (let [column-id-bytes (coerce-to-byte-string column-id)
          value-bytes-or-long (or (maybe-coerce-to-byte-string value) value)]
      (.setCell
       mutation-api-obj
       family-id
       column-id-bytes
       0 ;; timestamp
       value-bytes-or-long))))

(defn- row-map->RowMutation
  [table-id row-key row-map]
  (let [row-mutation
        (RowMutation/create table-id (coerce-to-byte-string row-key))]
    (set-cells-from-row-map row-mutation row-map)
    row-mutation))

(defn- row-map->Mutation
  [row-map]
  (let [mutation (Mutation/create)]
    (set-cells-from-row-map mutation row-map)
    mutation))

(defn set-row-async
  "Sets the contents of multiple cells in one row. The shape of row should be
   a map where keys are [column-family-id column-id] and values are byte
   strings.

   Like all operations in this API, the timestamps of all writes will be set to
   zero. The ability to specify a timestamp may be added in the future if we
   need it.

   If you want to write multiple rows, this is inefficient. Use set-rows-async
   instead."
  [^BigtableDataClient client
   ^String table-id
   row-key
   row]
  {:pre [client (string? table-id)]}
  (let [row-mutation (row-map->RowMutation table-id row-key row)]
    (.mutateRowAsync client row-mutation)))

(defn set-rows-async
  "Given a collection of [row-key row-map] pairs, insert all the rows into
   the given table using a BulkMutation. This is the most efficient way to
   set many rows simultaneously. Performance will be best if the row-keys are
   lexicographically adjacent, because then the request can be
   handled by a single BigTable node."
  [^BigtableDataClient client
   ^String table-id
   rows]
  {:pre [client (string? table-id)]}
  (let [bulk-mutation (BulkMutation/create table-id)]
    (doseq [[row-key row-map] rows]
      (.add bulk-mutation
            (coerce-to-byte-string row-key)
            (row-map->Mutation row-map)))
    (.bulkMutateRowsAsync bulk-mutation)))

(defn- ->row-ResponseObserver
  "Returns [chan, response-observer], where response-observer is
   a ResponseObserver<Row> that pushes rows onto chan."
  []
  (let [chan (async/chan 100
                         (map (fn [r]
                                (if (instance? Throwable r)
                                   r
                                   [(.toByteArray (.getKey r))
                                    (Row->map r)]))))]
    [chan
     (reify ResponseObserver
       (onComplete [this]
         (async/close! chan))
       (onError [_this err]
         (async/put! chan err)
         (async/close! chan))
       (onResponse [_this response]
         (async/put! chan response))
       (onStart [_this _controller]))]))

(defn- row-chan->lazy-seq
  [chan]
  (when-let [x (async/<!! chan)]
    (if (instance? Throwable x)
      (throw x)
      (cons x (lazy-seq (row-chan->lazy-seq chan))))))

(defn- ->Query
  "Constructs a query from the map passed to get-row-range.
   TODO: support Filters."
  [table-id {:keys [prefix start end limit row-keys]}]
  (cond-> (Query/create table-id)
    prefix (.prefix (coerce-to-byte-string prefix))
    (and start end) (.range (coerce-to-byte-string start)
                            (coerce-to-byte-string end))
    limit (.limit limit)
    (not-empty row-keys) (#(reduce
                            (fn [q k] (.rowKey q (coerce-to-byte-string k)))
                            %
                            row-keys))))

(defn query-rows
  "Query a range of rows by keys. The specification of the range can be made
   using a :prefix, or by :start and :end byte arrays. :limit, an integer,
   must be supplied. A set of exact keys :row-keys can be supplied, too.

   Returns a lazy seq of results, which are streamed from BigTable."
  [^BigtableDataClient client
   ^String table-id
   query-spec]
  (let [[chan response-observer] (->row-ResponseObserver)]
    (.readRowsAsync client (->Query table-id query-spec) response-observer)
    (row-chan->lazy-seq chan)))

(defn delete-row-async
  "Delete the specified row. Returns a future which resolves to null when
   the row has been deleted."
  [^BigtableDataClient client
   ^String table-id
   row-key]
  (let [row-mutation (.deleteRow
                      (RowMutation/create
                       table-id (coerce-to-byte-string row-key)))]
    (.mutateRowAsync client row-mutation)))
