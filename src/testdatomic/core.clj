(ns testdatomic.core
  (:require [datomic.api :as d]
            [clj-time.core :as t]
            [clj-time.coerce :as ct])
  (:gen-class))

(def db-uri "datomic:sql://testdatomic?jdbc:postgresql://localhost:5432/testdatomic?user=datomic&password=datomic")

(d/create-database db-uri)

(def conn (d/connect db-uri))

(def key-idents
  [{:db/ident :plan} {:db/ident :fact} {:db/ident :inflow} {:db/ident :outflow}])

(def dimension-schema
  [{:db/ident :dim-group/name
    :db/doc "Название группы измерения"
    :db/valueType :db.type/string
    :db/fulltext true
    :db/cardinality :db.cardinality/one}
   {:db/ident :dimension/name
    :db/doc "Название измерения"
    :db/valueType :db.type/string
    :db/fulltext true
    :db/cardinality :db.cardinality/one}
   {:db/ident :dimension/group
    :db/doc "Группа, к которой относится измерение"
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}])

(def entry-schema
  [{:db/ident :entry/summ
    :db/doc "Сумма записи"
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :entry/date
    :db/doc "Дата записи"
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :entry/v-flow
    :db/doc "Тип потока записи (приток/отток)"
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :entry/v-type
    :db/doc "Тип записи (факт/план)"
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :entry/dims
    :db/doc "Измерения записи"
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

(def dimensions-data
  [{:dim-group/name "Контрагенты"}
   {:dim-group/name "Договоры"}
   {:dim-group/name "Счета"}
   {:dim-group/name "Статьи"}
   {:dim-group/name "Проекты"}])


;; queries
;; (deref (d/transact conn dimensions-data))
(def db (d/db conn))


(defn eid-of-dim-group
  [dim-group-name]
  (-> (d/q '[:find ?e
             :in $ ?group-name
             :where [?e :dim-group/name ?group-name]]
           db dim-group-name)
      ffirst))

(eid-of-dim-group "Контрагенты")


(def dimensions-examples
  [{:dimension/name "Реклама.кз"
    :dimension/group (eid-of-dim-group "Контрагенты")}
   {:dimension/name "Иванов И.И"
    :dimension/group (eid-of-dim-group "Контрагенты")}
   {:dimension/name "ТОО Вектор"
    :dimension/group (eid-of-dim-group "Контрагенты")}
   {:dimension/name "Договор 1"
    :dimension/group (eid-of-dim-group "Договоры")}
   {:dimension/name "дог. от 15.04.2017"
    :dimension/group (eid-of-dim-group "Договоры")}
   {:dimension/name "Основной счет"
    :dimension/group (eid-of-dim-group "Счета")}
   {:dimension/name "Валютный счет"
    :dimension/group (eid-of-dim-group "Счета")}])

(eid-of-dim-group "Контрагенты")

(d/q '[:find ?e
       :where [?e :dimension/group 17592186045433]]
     db)

(->> #{[17592186045441] [17592186045439] [17592186045440]}
     (map first)
     (into []))

(defn dimension-ids-of-group
  [group-name]
  (->> (d/q '[:find ?e
              :in $ ?group-id
              :where [?e :dimension/group ?group-id]]
            db (eid-of-dim-group group-name))
       (map first)
       (into [])))

(dimension-ids-of-group "Контрагенты")

(d/pull (d/db conn) [:db/id :dimension/name {:dimension/group [:db/id :dim-group/name]}]
        17592186045441)


(def test-ents
  (d/pull-many db [:db/id :dimension/name {:dimension/group [:db/id :dim-group/name]}]
               (dimension-ids-of-group "Счета")))

test-ents

(->> test-ents
     (group-by :dimension/group))
;; (deref (d/transact conn dimensions-examples))

(defn jdate
  [year month day]
  (-> (t/date-time year month day)
      (ct/to-date)))

(->> (d/q '[:find ?e
            :where [?e :dim-group/name]]
          db)
     (map first))


(defn replace-all-keys
  "Заменить все ключи в объекте на другие"
  [rm x]
  (clojure.walk/postwalk
    (fn [el]
      (if (and (keyword? el)
               (contains? rm el))
        (get rm el)
        el))
    x))


(defn all-dim-groups-w-dimensions
  []
  (->> db
       (d/q '[:find (pull ?e [:db/id :dim-group/name {:dimension/_group [:db/id :dimension/name]}])
              :where [?e :dim-group/name]])
       (map first)
       (replace-all-keys {:db/id :id :dim-group/name :name :dimension/_group :dims :dimension/name :name})))


(def all-dimensions-w-names
  (->> (d/q '[:find ?e ?dim-name
              :where [?e :dimension/name ?dim-name]]
            db)
       (reduce (fn [m [id name]]
                 (assoc m id {:id id :name name}))
               {})))

all-dimensions-w-names


(def entries
  [{:entry/summ 5000.00
    :entry/date (jdate 2017 4 17)
    :entry/v-flow :inflow
    :entry/v-type :fact
    :entry/dims [17592186045439 17592186045442]}
   {:entry/summ 12000.00
    :entry/date (jdate 2017 3 17)
    :entry/v-flow :outflow
    :entry/v-type :fact
    :entry/dims [17592186045441 17592186045445]}
   {:entry/summ 500000.00
    :entry/date (jdate 2017 5 22)
    :entry/v-flow :inflow
    :entry/v-type :plan
    :entry/dims [17592186045440 17592186045444]}])

;; (deref (d/transact conn entries))


;; (deref (d/transact conn [[:db/retract 17592186045448 :entry/dims 17592186045441]
;;                          [:db/add "datomic.tx" :db/doc "Убрали измерение из второй записи"]]))


(defn get-all-entries
  "Получить все записи за все время"
  []
  (->> (d/q '[:find (pull ?e [:db/id :entry/summ :entry/date {:entry/dims [:db/id :dimension/name]} {:entry/v-flow [:db/ident]} {:entry/v-type [:db/ident]}])
              :where [?e :entry/summ]]
            (d/db conn))
       (map first)
       (replace-all-keys {:db/id :id :entry/summ :summ :entry/date :date :entry/dims :dims
                          :dimension/name :name :entry/v-flow :v-flow :entry/v-type :v-type})))

(get-all-entries)



(defn get-entries-between-dates
  "Получить все записи между двумя датами"
  [date-from date-to]
  (->> (d/q '[:find (pull ?e [:db/id :entry/summ :entry/date {:entry/dims [:db/id :dimension/name]} {:entry/v-flow [:db/ident]} {:entry/v-type [:db/ident]}])
              :in $ ?date-from ?date-to
              :where [?e :entry/summ]
              [?e :entry/date ?cur-date]
              [(<= ?cur-date ?date-to)]
              [(> ?cur-date ?date-from)]]
            (d/db conn) date-from date-to)
       (map first)
       (replace-all-keys {:db/id :id :entry/summ :summ :entry/date :date :entry/dims :dims
                          :dimension/name :name :entry/v-flow :v-flow :entry/v-type :v-type})))

(get-entries-between-dates (jdate 2017 4 15) (jdate 2018 4 17))










;;;; =================================
;;;; TEST
;; (def system-ns #{"db" "db.type" "db.install" "db.part"
;;                  "db.lang" "fressian" "db.unique" "db.excise"
;;                  "db.cardinality" "db.fn"})


;; (d/q '[:find ?e ?ident
;;        :in $ ?system-ns
;;        :where
;;        [?e :db/ident ?ident]
;;        [(namespace ?ident) ?ns]
;;        [((comp not contains?) ?system-ns ?ns)]]
;;      db system-ns)









(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
