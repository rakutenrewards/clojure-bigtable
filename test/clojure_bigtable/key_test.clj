(ns clojure-bigtable.key-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure-bigtable.key :as key]))

(def test-value 123)

(def test-value-str "123")

(deftest test-padding
  (doseq [i (range 0 (count test-value-str))]
    (testing (str "pad less than string length is identity: " i)
      (is (= test-value-str (#'key/pad i test-value)))))
  (doseq [i (range (inc (count test-value-str)) 20)]
    (testing (str "pad greater than string length is padded: " i)
      (let [padded (#'key/pad i test-value)]
        (is (= i (count padded)))
        (is (str/ends-with? padded test-value-str))
        (is (= \0 (first padded)))))))

(defn- sane-str-compare
  "Like compare, but without the undocumented and bizarre behavior for strings."
  [x y]
  (compare (compare x y) 0))

(deftest test-build
  (testing "build key from values"
    (is (= "00000000000000000123#hello#world" (key/build [123 "hello" "world"])))
    (is (= "09223372036854775684#hello#world" (key/build [(key/invert 123) "hello" "world"]))))
  (testing "lexicographic ordering of integer keys"
    (doall
     (repeatedly
      100
      #(let [x (rand-int 100)
             y (rand-int 100)
             k1 (key/build ["hello" x])
             k2 (key/build ["hello" y])
             k1- (key/build ["hello" (key/invert x)])
             k2- (key/build ["hello" (key/invert y)])
             expect (compare x y)]
         (is (= expect (sane-str-compare k1 k2)))
         (is (= (unchecked-negate expect) (sane-str-compare k1- k2-))))))))
