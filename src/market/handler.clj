(ns market.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [cheshire.core :as json]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]))

(import 'java.security.MessageDigest
        'java.math.BigInteger)

(defmacro ignore-errors [err & body]
  (let [e (gensym)]
    `(try ~@body (catch Exception ~e (println (.getMessage ~e)) ~err))))

(def lstatus {0 "Success"
              101 "Shutdown in process"
              401 "Missing parameter"
              402 "Incorrect data"
              403 "Incorrect parameter"
              404 "Incorrect amount"
              405 "Incorrect address"
              406 "Incorrect currency"})

(def currencies ["EUR" "USD" "BTC" "USDT"])



(defn md5 [s]
  (let [algorithm (MessageDigest/getInstance "MD5")
        size (* 2 (.getDigestLength algorithm))
        raw (.digest algorithm (.getBytes s))
        sig (.toString (BigInteger. 1 raw) 16)
        padding (apply str (repeat (- size (count sig)) "0"))]
    (str padding sig)))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defmacro sleep [x] `(Thread/sleep (* ~x 1000)))

(def shutdown (atom 0))
(def addr-file "/Users/stephane/tmp/addr")
(def wallet-file "/Users/stephane/tmp/wallet")
(def event-file "/Users/stephane/tmp/event.log")

(def addr (ref (ignore-errors {} (read-string (slurp addr-file)))))
(def wallet (ref (ignore-errors {} (read-string (slurp wallet-file)))))

(def action (atom ()))

(defmacro now [] `(str (java.util.Date.)))

(defn exit []
  (future (do
            (swap! shutdown inc)
            (swap! action conj "save-addr")
            (sleep 10)
            (shutdown-agents)
            (System/exit 0)))
  "Shutdown in 10 seconds")


(defn save-event [event-type txid addr currency amount info]
  (spit event-file (format "%s,%s,%s,%s,%s,%s,%s\n" (now) event-type txid addr currency amount info) :append true))


(defn json-answer [params h error-code]
  (if (= @shutdown 0)
    {:status 200
     :headers {"Content-Type" "Application/json charset=utf-8"}
     :body (json/generate-string (conj h
                                       [:status error-code]
                                       [:status-txt (get lstatus error-code)]))}
    {:status 503
     :headers {"Content-Type" "Application/json charset=utf-8"}
     :body (json/generate-string (conj [:status 101
                                        :status-txt (get lstatus 101)]))}))

(defn generate-random-hash []
  (let [a (transient [])]
    (dotimes [_ 12]
      (conj! a (rand-int Integer/MAX_VALUE)))
    (md5 (str (persistent! a)))))

(defn generate-addr []
  (let [new-addr (str "1" (generate-random-hash))
        txid (str "x" (generate-random-hash))]
    (dosync (alter addr (fn [x] (conj x [new-addr 1]))))
    (swap! action conj ["save-addr" txid new-addr])
    new-addr))

(defn save-addr []
  (spit addr-file (str @addr)))

(defn credit [json]
  (let [content (ignore-errors [] (json/parse-string json))
        txid (str "x" (generate-random-hash))]
    (cond (nil? content) (json-answer {} {} 402)
          (or
           (nil? (content "to"))
           (nil? (content "amount"))
           (nil? (content "currency"))) (json-answer {} {} 401)
          (not (number? (content "amount"))) (json-answer {} {} 404)
          (not (@addr (content "to"))) (json-answer {} {} 405)
          (not (seq (filter  (fn [x] (= (content "currency") x))  currencies))) (json-answer {} {} 406)
          true (do
                 (swap! action conj ["credit" txid (content "to") (content "currency") (content "amount")])
                 (json-answer {} {"txid" txid} 0)))))
                                       

(future
  (while true
    (cond (= (first (last @action)) "save-addr") (do
                                                   (println "Save address")
                                                   (save-addr)
                                                   (save-event "new" (nth (last @action) 1) (nth (last @action) 2) "" "" "")
                                                   (swap! action butlast))
          true (sleep 1))))


(defroutes app-routes
  (GET "/" [] "Hello World")
  (GET "/addrs"       {params :query-params} (json-answer params {:addrs (keys @addr)} 0))
  (GET "/new-addr"    {params :query-params} (json-answer params (and (= @shutdown 0 ) {:addr (generate-addr)}) 0))
  (GET "/currencies"  {params :query-params} (json-answer params {:currencies currencies} 0))
  (GET "/shutdown"    {params :query-params} (json-answer params {:shutdown (exit)} 0))
  (GET "/actions"     {params :query-params} (json-answer params {:actions @action} 0))
  (POST "/credit"     {body :body} (if (= @shutdown 0)
                                     (let [b (slurp body)]
                                       (println b)
                                       (credit b))
                                     (json-answer body 0)))
  (GET "/param"       {params :query-params} (str params))
  (POST "/param"      {body :body} (let [b (slurp body)] b))
  (route/not-found "Not Found"))



(def app
  (wrap-defaults app-routes (assoc-in site-defaults [:security :anti-forgery] false)))




    
  



