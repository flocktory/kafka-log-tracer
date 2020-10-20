(defproject com.flocktory.component/kafka-log-tracer "0.0.2"
  :description "Log tracer for kafka consumer"
  :url "https://github.com/flocktory/kafka-log-tracer"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.flocktory/kafka-consumer "0.0.9"]]
  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :sign-releases false}]])
