(defproject cfm-lfm-importer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main cfm-lfm-importer.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-http "3.9.1"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.527"]
                 [com.google.guava/guava "27.1-jre"]]
  :repl-options {:init-ns cfm-lfm-importer.core})
