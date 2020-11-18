(ns clojure-bigtable.key
  "Utility functions to construct Bigtable keys from tuples, to aid in indexing
   data in such a way that range reads can read related data."
  (:require [clojure.string :as str]))

(defn invert
  "Given an integer key element, subtract it from MAX_VALUE so that range
   reads can start at the largest (or most recent) value."
  [x]
  (- java.lang.Long/MAX_VALUE x))

(defn- pad
  [pad-length x]
  (if (number? x)
    (let [s (str x)
          n (count s)]
      (str (apply str (take (- pad-length n) (repeat \0))) s))
    (str x)))

(defn build
  "(build pad-length arg1 arg2 ... arg-n) constructs a key out of args 1 to n.
   Each integer key whose string representation is less than pad-length will be
   padded at the front with zeros so that its length is equal to pad-length.
   This ensures that lexicographical ordering is maintained for keys of uneven
   length.

   If pad-length is not provided, the default pad length will be long enough to
   fit all 64-bit signed integers. Using this behavior is recommended."
  ([key-elements]
   (build 20 key-elements))
  ([pad-length key-elements]
   {:pre [(every? some? key-elements)]}
   (str/join "#" (map #(pad pad-length %) key-elements))))
