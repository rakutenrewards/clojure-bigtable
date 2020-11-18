(ns clojure-bigtable.core-test
  (:require [clojure.test :refer :all]
            [medley.core :as medley]
            [clojure-bigtable.admin :as admin]
            [clojure-bigtable.core :as core]
            [clojure-bigtable.key :as key]))

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

(def k1 (byte-array [1 2 3]))
(def k2 (byte-array [1 2 4]))
(def k3 (byte-array [2 3 4]))

(def k-ordered-1 (key/build ["tenant1" "user1" (key/invert 123)]))
(def k-ordered-2 (key/build ["tenant1" "user1" (key/invert 4569)]))
(def k-ordered-3 (key/build ["tenant1" "user1" (key/invert 5673)]))
(def k-ordered-4 (key/build ["tenant1" "user2" (key/invert 4570)]))
(def k-ordered-5 (key/build ["tenant1" "user2" (key/invert 5000)]))

(use-fixtures :each
  (fn [f]
    (f)
    @(core/delete-row-async client table k1)
    @(core/delete-row-async client table k2)
    @(core/delete-row-async client table k3)
    @(core/delete-row-async client table k-ordered-1)
    @(core/delete-row-async client table k-ordered-2)
    @(core/delete-row-async client table k-ordered-3)
    @(core/delete-row-async client table k-ordered-4)
    @(core/delete-row-async client table k-ordered-5)))

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

(deftest test-inverted-numeric-range
  (testing "Return inverted numeric keys in descending order"
    @(core/set-rows-async
      client
      table
      [
       ;; tenant 1, user 1, ascending "timestamp" keys
       [k-ordered-1 {["fam1" "col1"] (byte-array [4])}]
       [k-ordered-2 {["fam1" "col1"] (byte-array [5])}]
       [k-ordered-3 {["fam1" "col1"] (byte-array [6])}]

       ;; tenant 1, user 2, ascending "timestamp" keys, occurring
       ;; concurrently with tenant 1, user 1's keys.
       [k-ordered-4 {["fam1" "col1"] (byte-array [1])}]
       [k-ordered-5 {["fam1" "col1"] (byte-array [2])}]])
    (let [result (->> (core/query-rows
                       client
                       table
                       {:prefix (key/build ["tenant1" "user1"])
                        :limit 2})
                      (map second)
                      (map #(get % ["fam1" "col1"])))]
      (is (= [[6] [5]] result)))))
