(ns clojure-bigtable.admin
  "Admin interface for BigTable -- CRUD for tables, backups, etc."
  (:import
   (com.google.cloud.bigtable.admin.v2 BigtableTableAdminClient)
   (com.google.cloud.bigtable.admin.v2.models CreateTableRequest)))

(defn create-admin-client
  [project-id instance-id]
  {:pre [(string? project-id) (string? instance-id)]}
  (BigtableTableAdminClient/create project-id instance-id))

(defn create-table
  "Creates a table with the specified id and seq of family-ids"
  [admin-client table-id family-ids]
  (let [request (CreateTableRequest/of table-id)]
    (doseq [family-id family-ids]
      ;;TODO: support for GCRules
      (.addFamily request family-id))
    (.createTable admin-client request)))
