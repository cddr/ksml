# ksml

Ksml is a library for representing kafka streams topologies in Clojure. It uses
vectors to represent processing nodes and translates simple Clojure functions
into objects that implement the corresponding Kafka interfaces.

Make big data as easy as HTML.

## Usage

```clojure
(ns com.bigdata.app
  (:require
    [cddr.ksml.core :refer [ksml]]))

(def basic-join
  (ksml [:join (fn [left right]
                 {:left left, :right right})
          [:stream #"foo"]
          [:stream #"bar"]]))
```          

## Bugs

There are probably lots of bugs. This is mostly untested at the moment. Will try
to add some specs and flush them out.

Credit to @ztellman for the idea and hiccup, sablono and SICP for implementation
ideas.

## License

Copyright © 2017 Andy Chambers

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
