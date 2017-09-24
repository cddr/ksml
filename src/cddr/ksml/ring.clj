(ns cddr.ksml.ring
  "A Kafka Streams application consists of a set of self-coordinating processes
   that distribute the work of processing one or more input topics. Each process
   typically only has access to data pertaining to a sub-set of the entire dataset.

   The KafkaStreams object keeps track of the mapping between the host of each
   process and it's corresponding keys in local storage. This lets us discover
   at request-time the host that is capable of handling some HTTP request that
   is backed by the distributed local storage of a kafka streams application.

   The handler defined in this namespace either handles the request locally if it
   can, or if necessary forwards the request on to the remote host and responds
   passes the response onto the original requestor")

(defn remote?
  [location self]
  (= (select-keys location [:server-name :server-port])
     (select-keys self [:server-name :server-port])))

(defn handler
  "Return a ring handler that locates the state required to serve a response
   and delegates the request to a handler with access to the the state.

   To support a wide variety of use-cases, this handler takes a map with the
   following keyword parameters

   :find-host   Finds the location of the state required to fulfill this
                request. The returned location should be a map with the keys
                :server-name and :server-port

   :remote      If the state is determined to be located elsewhere, this
                function will be invoked to serve a response. It accepts
                a location, and the original request


   :local       If the state is determined to be located on this instance,
                the `local` function will be invoked to serve a response. It
                accepts the original `request`, and the specified `streams`
                object."
  [{:keys [find-host
           remote
           local
           streams]}]
  (fn [request]
    (let [location (find-host streams request)]
      (if (remote? location request)
        (-> request
            (assoc :location location)
            (remote))

        (-> request
            (assoc :streams streams)
            (local))))))
