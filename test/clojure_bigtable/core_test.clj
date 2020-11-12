(ns clojure-bigtable.core-test
  (:require [clojure.test :refer :all]
            [clojure-bigtable.admin :as admin]
            [clojure-bigtable.core :as core]))

;; NOTE: to run tests, set up the emulator on your machine
;; https://cloud.google.com/bigtable/docs/emulator

(def client (core/create-client "fake-project" "test-instance-id"))
(def admin-client (admin/create-admin-client "fake-project" "test-instance-id"))

;;TODO: real fixtures
(def table "test-table")
(try
  (admin/create-table admin-client table ["fam1" "fam2"])
  (catch Exception e nil))

;; TODO: each fixture to delete k1 k2 k3

(def k1 (byte-array [1 2 3]))
(def k2 (byte-array [1 2 4]))
(def k3 (byte-array [2 3 4]))

(def example-row
  {["fam1" "col1"] {:value (byte-array [1 3 3 7])}
   ["fam1" "col2"] {:value (byte-array [1 3 3])}})

(def example-row-2
  (assoc-in example-row [["fam1" "col1"] :value] (byte-array [7 3 3 1])))

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
        (is (= [7 3 3 1] (into [] (get-in row [["fam1" "col1"] :value]))))))))
