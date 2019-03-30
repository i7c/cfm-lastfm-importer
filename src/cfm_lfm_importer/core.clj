(ns cfm-lfm-importer.core
  (:use [clojure.walk])
  (:require [clj-http.client :as client])
  (:require [clojure.data.json :as json])
  (:import (com.amazonaws.regions Regions))
  (:import (com.amazonaws.services.dynamodbv2 AmazonDynamoDBClientBuilder))
  (:import (com.amazonaws.services.dynamodbv2.document DynamoDB Item))
  (:gen-class))

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

(defn get-tracks [user]
  (flatten (map #(get-in % ["recenttracks" "track"])
                (for [page (drop 1 (range))
                      :let [chunk (get-track-chunk user page)
                            body (json/read-str (:body chunk))
                            totalPages (parse-int (get-in body ["recenttracks" "@attr" "totalPages"]))]
                      :while (< page totalPages)] body))))

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
                   :lastfm-user user
                   :artist-mbid (get-in scrobble ["artist", "mbid"])
                   :album-mbid (get-in scrobble ["album" "mbid"])
                   :track-mbid (get-in scrobble ["mbid"])}))))

(def table-playbacks
  (-> (AmazonDynamoDBClientBuilder/standard)
      (.withRegion (Regions/EU_WEST_1))
      .build
      DynamoDB.
      (.getTable "playbacks")))

(defn -main [& args]
  (->> (first args)
       (get-tracks)
       (remove #(get-in % ["@attr", "nowplaying"]))
       (map (partial scrobble-to-playback (first args)))
       (map stringify-keys)
       (map #(Item/fromMap %))
       (map #(.putItem table-playbacks %))
       (doall)))
