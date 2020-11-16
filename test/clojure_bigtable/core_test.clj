(ns clojure-bigtable.core-test
  (:require [clojure.test :refer :all]
            [medley.core :as medley]
            [clojure-bigtable.admin :as admin]
            [clojure-bigtable.core :as core]))

;; NOTE: to run tests, set up the emulator on your machine
;; https://cloud.google.com/bigtable/docs/emulator

(def client (core/create-client "fake-project" "test-instance-id"))
(def admin-client (admin/create-admin-client "fake-project" "test-instance-id"))

(def table "test-table")

(use-fixtures :once
  (fn [f]
    ;; ensure test table exists, ignore exception if already exists.
    (try
      (admin/create-table admin-client table ["fam1" "fam2"])
      (catch Exception _ nil))
    (f)))

;; TODO: each fixture to delete k1 k2 k3

(def k1 (byte-array [1 2 3]))
(def k2 (byte-array [1 2 4]))
(def k3 (byte-array [2 3 4]))

(use-fixtures :each
  (fn [f]
    @(core/delete-row-async client table k1)
    @(core/delete-row-async client table k2)
    @(core/delete-row-async client table k3)
    (f)))

(def example-row
  {["fam1" "col1"] (byte-array [1 3 3 7])
   ["fam1" "col2"] (byte-array [1 3 3])})

(def example-row-vecs
  (medley/map-vals #(into [] %) example-row))

(def example-row-2
  (assoc example-row ["fam1" "col1"] (byte-array [7 3 3 1])))

(def example-row-2-vecs
  (medley/map-vals #(into [] %) example-row-2))

(def example-row-3
  (assoc example-row ["fam1" "col3"] (byte-array [7 3 3 1 7])))

(def example-row-3-vecs
  (medley/map-vals #(into [] %) example-row-3))

(deftest test-row-exists
  (testing "row-exists: DNE"
    @(core/delete-row-async client table k1)
    (is (= false (deref (core/exists-async client table k1)))))
  (testing "row-exists"
    @(core/set-row-async client table k1 example-row)
    (is (= true (deref (core/exists-async client table k1))))))

(deftest test-set-row-cell
  (testing "set-row-cell: update existing row"
    @(core/set-row-async client table k1 example-row)
    (let [bytes-value (byte-array [7 3 3 1])]
      @(core/set-row-cell-async
        client
        table
        k1
        "fam1"
        "col1"
        bytes-value)
      (let [row @(core/read-row-async client table k1)]
        (is (= [7 3 3 1] (into [] (get row ["fam1" "col1"]))))))))

(deftest test-set-get-row
  (testing "set and get row"
    @(core/set-row-async client table k1 example-row)
    (let [result @(core/read-row-async client table k1)]
      (is (= example-row-vecs
             result)))))

(deftest test-set-get-rows
  (testing "set and get several rows, excluding keys outside :prefix"
    @(core/set-rows-async client table [[k1 example-row]
                                        [k2 example-row-2]
                                        [k3 example-row-3]])
    (let [result (core/query-rows client table {:prefix (byte-array [1 2])
                                                :limit 10})]
      (is (= #{example-row-vecs example-row-2-vecs} (into #{} (map second result)))))))
