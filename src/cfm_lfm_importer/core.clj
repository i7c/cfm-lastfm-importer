(ns cfm-lfm-importer.core
  (:use [clojure.walk])
  (:require [clj-http.client :as client]
            [clojure.data.json :as json])
  (:import (com.amazonaws.regions Regions))
  (:import (com.amazonaws.services.dynamodbv2 AmazonDynamoDBClientBuilder))
  (:import (com.amazonaws.services.dynamodbv2.document DynamoDB Item ItemUtils))
  (:import (com.amazonaws.services.dynamodbv2.model BatchWriteItemRequest PutRequest WriteRequest))
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestHandler]))

(def sha256 (com.google.common.hash.Hashing/sha256))

(defn sha256sum [s] (str (.hashString sha256 s java.nio.charset.StandardCharsets/UTF_8)))

(defn parse-int [number-string] (try (Integer/parseInt number-string) (catch Exception e nil)))

(defn get-track-chunk [user page]
  (client/get "https://ws.audioscrobbler.com/2.0/"
              {:throw-exceptions true
               :query-params {"method" "user.getrecenttracks"
                              "format" "json"
                              "user" user
                              "api_key" (System/getenv "LASTFM_KEY")
                              "page" page
                              "limit" 200}
               :as :json}))

(defn get-page [body] (parse-int (get-in body ["recenttracks" "@attr" "page"])))

(defn get-total-pages [body] (parse-int (get-in body ["recenttracks" "@attr" "totalPages"])))

(defn get-tracks [user page]
  (flatten
   (get-in
    (->> (get-track-chunk user page)
         (:body)
         (json/read-str))
    ["recenttracks" "track"])))

(defn scrobble-to-playback
  "Convert last.fm scrobbles to our own playback format"
  [user scrobble]
  (let [title (get-in scrobble ["name"])
        artist (get-in scrobble ["artist" "#text"])
        album (get-in scrobble ["album" "#text"])
        time (get-in scrobble ["date", "uts"])
        id (->> [user artist title album time]
                (map clojure.string/lower-case)
                (clojure.string/join "-")
                (sha256sum))]
    (into {}
          (remove #(let [val (second %)] (when (string? val) (clojure.string/blank? val)))
                  {:title title
                   :artist artist
                   :album album
                   :time (* 1000 (parse-int time))
                   :id id
                   :sort-key (clojure.string/join "-" [time id])
                   :lastfm-user user
                   :artist-mbid (get-in scrobble ["artist", "mbid"])
                   :album-mbid (get-in scrobble ["album" "mbid"])
                   :track-mbid (get-in scrobble ["mbid"])}))))

(def dynamo-db
  (-> (AmazonDynamoDBClientBuilder/standard)
      (.withRegion (Regions/EU_CENTRAL_1))
      .build))

(defn -handleRequest [this request context]
  (->> (get-tracks (get request "user") (get request "page"))
       (remove #(get-in % ["@attr", "nowplaying"]))
       (map (partial scrobble-to-playback (get request "user")))
       (map stringify-keys)
       (map #(ItemUtils/fromSimpleMap %))
       (map #(PutRequest. %))
       (map #(WriteRequest. %))
       (partition-all 25)
       (map #(BatchWriteItemRequest. {"playbacks" %}))
       (map #(.batchWriteItem dynamo-db %))
       (map #(.getUnprocessedItems %))
       (json/write-str)))
