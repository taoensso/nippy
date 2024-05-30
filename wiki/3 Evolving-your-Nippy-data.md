> This article is **community content** kindly contributed by a Nippy user (@Outrovurt)

This article describes a number of use cases where you need to make changes to your code which will have some impact on data you have already frozen using Nippy, and how best to manage each specific case. We will also discuss custom freezing and thawing.

It is assumed you have working knowledge of a good editor (e.g. emacs) and know how to start a Clojure REPL (e.g. via CIDER).

Throughout this article we will refer to serialization as *freezing*, and deserialization as *thawing*, which are the terms used by Nippy. (The word "thawer" will be used throughout the article, it's obviously made up, but please bear with it!)

# Project setup

If you want to follow along, you can create a fresh project using whichever Clojure project management tool you are currently most comfortable with (lein, Clojure deps), and [include Nippy in the dependencies](https://github.com/taoensso/nippy/wiki#setup). Create the following namespaces:

* nippy.evolve
* nippy.other

and delete any default code which is generated for you by your project management tool.

We will start in `nippy.evolve`. Ensure your `(ns ...)` looks like this:

```clojure
(ns nippy.evolve
  (:require
   [taoensso.nippy
    :refer (freeze
            thaw)]))
```

Start a REPL and switch to `nippy.evolve` if you aren't already there.

# Freezing a record

Create the following record, either in your source code or in the REPL, and create a new instance of it:

```clojure
(defrecord FirstRec [])
(def x1 (FirstRec.))
```

Now freeze it:

```clojure
(def f1 (freeze x1))
```

The first thing you'll notice is that you don't have to set anything up at all, Nippy will freeze the record instance out of the box. It does this by determining that `x1` is a record, and uses a built-in freezer to freeze it.

## A frozen record

Let's take a quick look at `f1` at the REPL:

```clojure
nippy.evolve> (type f1)
[B
nippy.evolve> f1
[78, 80, 89, 0, 48, 21, 110, 105, 112, 112, 121, 46, 101, 118, 111,
 108, 118, 101, 46, 70, 105, 114, 115, 116, 82, 101, 99, 19]
```

The "[B" indicates that this is a Java Byte array, which you can also create using the Clojure function `byte-array`.

The value of `f1` is an array of bytes, consisting of two parts:

* the envelope
  - a 4-byte header, followed by
  - a 1-byte type id
* the packet itself, whatever you are encoding

The header provides sanity checking, the Nippy version number that created it, and information related to any compression or encryption employed. We won't go into detail here as to how the header is composed as it is not relevant to our discussion.

The type id tells us what type of data we have frozen. A positive value indicates a built-in type. In this case we have 48, which refers to a record, specifically a "small record" (small referring to the total number of bytes used to encode the body of the record.) This is all explained in the Nippy source code, where you can find a list of all built-in types that Nippy recognizes together with the type id used to represent it. In the latest version of Nippy this list can be found in the var `public-types-spec`.

So when we froze an instance of `FirstRec`, we used a built-in freezer for records to produce the above byte array.

What about the rest of the array? There are a total of 28 bytes, 5 of which are for the envelope, so the remaining 23 bytes are for the packet itself. But wait a minute. If we are basically freezing an empty record with no fields, why do we need 23 bytes to freeze it? The answer is that while the envelope encodes the fact that the remaining data is for a record, it also needs to encode the type of record that we have frozen. Otherwise there is no way to thaw it back to a record of type `FirstRec`. Since all envelopes contain 5 bytes, this information can only be encoded in one place: the packet itself.

So if you type the following at the REPL, you'll get a clue as to what is happening:

```clojure
nippy.evolve> (freeze "nippy.evolve.FirstRec")
[78, 80, 89, 0, 105, 21, 110, 105, 112, 112, 121, 46, 101, 118, 111,
 108, 118, 101, 46, 70, 105, 114, 115, 116, 82, 101, 99]
```

Once again the first 4-bytes represent the header. The fifth byte, 105, tells us that this is a string (specifically a "small" string of under 128 bytes), and the remaining packet is a 22-byte array:

`[21, 110, 105, 112, 112, 121, 46, 101, 118, 111,
 108, 118, 101, 46, 70, 105, 114, 115, 116, 82, 101, 99]`

If you look back at `f1` above, you will see that of the 23 bytes of the packet, the first 22 bytes are identical to the 22 bytes of the frozen string above. In other words, Nippy encodes the fully-qualified record name at the very start of the packet itself.

That just leaves a single byte, 19, at the end of `f1`. You can probably guess what this represents, so let's try the following:

```clojure
nippy.evolve> (freeze {})
[78, 80, 89, 0, 19]
```

Here we have tried to freeze just an empty map, and that has produced what appears to be a byte array with just an envelope and no packet. Ignoring the header, the fifth byte is 19, which corresponds to an empty map (:map-0 in the code), so there is no need for any packet.

To summarise, when Nippy freezes an empty record, it encodes it with:

* a 4-byte header
* a 1-byte type-id of 48 indicated a small record
* a 23-byte packet, the first 22 bytes of which represent the string "nippy.evolve.FirstRec", and the final byte which represents an empty map

This of course contains all the information Nippy requires to thaw the data back to an instance of `FirstRec`.

## Thawing a record

Now let us turn to thawing. Enter the following code into your source file or into the REPL:

```clojure
(def t1 (thaw f1))
```

```clojure
nippy.evolve> t1
{}
nippy.evolve> (type t1)
nippy.evolve.FirstRec
```

Exactly as we expected, `t1` returns what appears to be an empty map (though this depends on how your REPL is set up), but when we examine its type, we find that is has correctly been thawed as a `nippy.evolve.FirstRec`. This is entirely due to the way Nippy has interpreted all the information provided in the envelope and packet, described in the previous section.

# Evolving your code

So without setting anything up at all in your project, you can see how simple it is just to use Nippy's `freeze` and `thaw` functions to serialize instances of any record you care to create. However, if you have been following the above discussion, you will probably have noted that there are a number of problems here, one or more of which you might even have run into at some stage:

* a number of bytes are used to encode the name of the record in the packet; in our example 22 bytes are used
* since the name of the record is encoded in the packet, this means that if we change the name of the record or move it to another namespace, then try to thaw a previously frozen byte array, the operation will since Nippy will be unable to match up the previously encoded with the now renamed or moved record

We will look at moving and renaming first, and then consider how we can reduce the number of bytes in a packet afterwards.

## Moving or renaming a record

If we want to move or rename a record, for all data previously frozen using the record before renaming/moving it, Nippy will no longer be able to thaw that data since it can no longer match the record name encoded in the packet with a class generated by the record. To be exact, if you try this within the same session, while the REPL is open, then even if you have moved/renamed a record, the old record and the compiled class associated with it will still be available. This is a consequence of the way Clojure compiles records, and even if you try to do an `(ns-unamp 'nippy.evolve 'FirstRec)`, it will still be there. So to better understand this issue, we are going to first save the frozen byte array `f1` to a file, as follows.

Update the (ns) form to include the following:

```clojure
(ns nippy.evolve
  (:require
   ...
   [clojure.java.io
    :refer (file
            output-stream
            input-stream)]
   ))
```

Now enter the following code into the REPL:

```clojure
nippy.evolve> (with-open [out (output-stream (file "./frozen-first-rec"))]
                (.write out f1))
nil
```

This will result in the file `./frozen-first-rec` being created in the top-level of your project. We will come back to this file subsequently.

Next, move `FirstRec` to the namespace `nippy.other`, and delete any code which references it within `nippy.evolve`:

```clojure
(ns nippy.other)

(defrecord FirstRec [])
```

Now quit the REPL, and start a new one. If not already there, change to namespace `nippy.evolve` and type the following:

```clojure
nippy.evolve> FirstRec
Syntax error compiling at (*cider-repl clojure/evolve:localhost:36735(clj)*:0:0).
Unable to resolve symbol: FirstRec in this context
```

You should see the above error, which shows that FirstRec is no longer defined in `nippy.evolve`.

Still from within `nippy.evolve`, type the following:

```clojure
nippy.evolve> (with-open [ina (input-stream (file "./frozen-first-rec"))]
                (let [buf (byte-array 28)
                      n   (.read in buf)]
                  (thaw buf)))
```

This code attempts to open the file `./frozen-first-rec` and read it into `buf`, a byte array. If you have been following everything exactly thus far, running the above should result in the following being returned:

```clojure
#:nippy{:unthawable
        {:type :record,
         :cause :exception,
         :class-name "nippy.evolve.FirstRec",
         :content {},
         :exception #error {...}}}
```

Once again, this is to be expected. We have tried to thaw a byte array corresponding to a record with a class-name of "nippy.evolve.FirstRec", which of course no longer exists as we have moved it to `nippy.other`.

Whenever you encounter a `:nippy/unthawable` as a result of thawing, one approach is to write custom code to fix it. For example in the above situation, you could parse the map, and for `:type :record`, `:class-name "nippy.evolve.FirstRec"`, you could then look at the `:content` and create that as a `nippy.other.FirstRec` record. If the `:nippy/unthawable` appears deeply nested within the returned structure, you could call `clojure.walk/prewalk` as a more general solution, and provide a mapping table of {<old-class-name-str> <new-class-name>}, in this case {"nippy.evolve.FirstRec" nippy.other.FirstRec}, and use that to create FirstRec records of the new type. In any cases where you don't have access to the frozen files containing the old format, for example where you have created a desktop application which saves files which contain a frozen data structure, this will be your only option. However, in cases where you do have access to the frozen data, there is an alternative, better approach.

## Custom freeze and thaw

Before we talk about custom freeze and thaw, it's worth taking a step back and looking at how each of these processes work from a high-level.

* **freezing** takes as its input a piece of data, and the process is driven entirely by the *type* of that data
* **thawing** takes as its input a piece of previously frozen data, and that process is driven by the *type-id* in the envelope and where appropriate some additional data in the packet, such as the name of the record's class for records

In general, any data we freeze, we want to be able to thaw back to its original form. In other words, the following should always hold true:

```clojure
(= data
   (-> data freeze thaw))
```

More accurately, at any given time we want to be able to restore any frozen data to its original state when we thaw it. Although this appears to be described by the above condition, there is a subtle but important distinction in that the above assumes that we are freezing and then thawing the data an instant later, whereas in reality *the thawing process can happen at any future time*. What this means is that when writing custom freeze and thaw code, it is important that only the thaw code matches the frozen data at any given time. This will become important soon.

In situations where we have access to previously frozen data, if we want to rename or move a record, we have an additional option to parsing the result of thaw and looking for the occurrence of any `:nippy/unthawable` maps nested in the resultant data (described above): custom freezing and thawing. Even if we have already frozen data using the built-in record freezer, we can still deal with this situation fairly easily. The trick is to understand that there is a sequence to be followed when it comes to implementing custom freezers and thawers.

In our above example, to avoid receiving the `:nippy/unthawable` result, we can start by moving the `defrecord` code back from `nippy.other` to `nippy.evolve`. Now if we evaluate the `nippy.evolve` namespace, then attempt to thaw the file we saved, we should get back our original data that we froze before, an instance of a record of type `nippy.evolve.FirstRec`. So far so good. Now the next step is to break the dependency between the frozen data and the name of the original record used to freeze it. To do this, we can write a custom freezer and thawer, and the best part is that we can have these active within nippy at the same time as the built-in record freezer/thawer. Here is how:

First, within `nippy.evolve`, update the `(ns)` form to include `extend-freeze` and `extend-thaw`:

```clojure
(ns nippy.evolve
  (:require
   [taoensso.nippy
    :refer (...
            extend-freeze
            extend-thaw
            freeze-to-out!
            thaw-from-in!)]
   ...
   ))
```

Ensure that the the following code is included in `nippy.evolve`:

```clojure
(defrecord FirstRec [])
```

Then add the following code:

```clojure
(extend-freeze FirstRec 1
               [x data-out]
               (freeze-to-out!
                data-out
                (into {}
                      (:data x))))

(extend-thaw 1 [data-input] (map->FirstRec (thaw-from-in! data-input)))
```

These two blocks create respectively a custom freezer for FirstRec, which writes out an envelope with custom id 1, and a custom thawer which is used only for packets with custom id 1. We can use any id in the range `1 <= id < = 127`. As stated at the start of this section, this code shows that the freezer is driven by the type of data being frozen, and the thawer is driven only by the custom id of the data being thawed.

Now evaluate the whole `nippy.evolve` namespace again. This will extend Nippy by providing it with a custom freeze and thaw for any instances of `FirstRec`:

* from this point onwards, any new data we create and freeze will be frozen using the custom freezer
* any newly frozen data will be thawed by the above custom thawer
* any previously frozen data, with type-id 48 for records (see above), will still be thawed by the built-in record thawer

This last point is important in that it allows us to simultaneously deal with legacy data while also being able to process new data.

Let's try freezing a new record:

```clojure
nippy.evolve> (freeze (FirstRec.))
[78, 80, 89, 0, -1, 19]
```

Once again we have our 4-byte header, but this time we have a negative number as our type-id. This is actually the negative of the custom-id we specified in our call to `extend-freeze`, and is how Nippy stores custom ids. We could have also used a (preferably namespaced) keyword, but that would have taken an extra 16-bits (a hash of the keyword) in the packet itself, and arguably doesn't provide any benefits over using an integer id. As long as we maintain a mapping between custom id and type in our code, and don't use the same custom id for a completely different type in the future, we shouldn't run into any issues with using a custom id over a keyword.

The packet in this case is just a single byte, 19, which refers to an empty map in nippy.clj type-ids. This map is then used by the thawer to reconstruct our original instance a `FirstRec`.

The first thing that should be evident is that this is much shorter, by 22-bytes, than the output produced by the built-in record freezer. This is because our custom freezer only stores the record as a map, with no string corresponding to the name of the record. This has resulted in decoupling the frozen data from any concrete record type (e.g. `nippy.evolve.FirstRec`), by instead coupling it to only an arbitrary custom id of our choosing (e.g. 1), and leaving us to provide the mapping between the custom id and some type within the custom thawer, which from now on we can do by updating the thawer with custom id 1.