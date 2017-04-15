(ns testdatomic.core
  (:require [datomic.api :as d])
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
