# clojure-bigtable

A Clojure library designed to ... well, that part is up to you.

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
         ;; Each value is a map containing at least :value, and optionally
         ;; a timestamp (each cell in BigTable is a timeseries).
         {["column_family_1" "column_1"] {:value (byte-array [ 4 5 6])}}))
;; => nil

;; Read row. Throws if table DNE.
(deref (core/read-row-async client "test-table" (byte-array [1 2 3])))
;; => {["column_family_1" "column_1"] {:value (byte-array [ 4 5 6])
;;                                     :timestamp <something>}}
```

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
