# mongodb-id-generator

Hi/Lo algorithm for MongoDB

## Basic idea

Hi/Lo algorithm is used in cases when you want to generate unique values like keys/ids on a client, not db side, and keep that sequence in sync for all clients. Clients may be processes/services, or just multiple repositories in an alone app process. The idea is to minimize amount of calls to the database, giving each client a range of ids per request. We can describe a range in two numbers called `high` and `low` values, where the `min` value equals `high * low` and the `max` equals `(high + 1) * low - 1`. So "renting" a range can be performed as a simple increment of a `high` value. Note that with such approach we can have gaps in our ids sequence with a maximum length of `low * n`, where `n` is a number of clients. That can be unpleasant, for example, when one client get very few requests and can't spent his first `[1, 50]` range, while the second is already finishing `[201, 250]`.

In MongoDB we have [findOneAndUpdate](https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndUpdate/#mongodb-method-db.collection.findOneAndUpdate) method, that can return a modified version of a document passing `returnNewDocument` parameter as `true`. MongoDB has so called [retryable writes](https://docs.mongodb.com/manual/core/retryable-writes/) concept so it seems that there are no need to worry about write conflicts, but I put this update operation into a transaction under the new retryable [callpack api](https://docs.mongodb.com/manual/core/transactions-in-applications/#std-label-txn-callback-api).

## Modification with renting on the client level

In my case I also needed an option to lock or "rent" an id with a possibility to return it to the pool later, for reuse.
