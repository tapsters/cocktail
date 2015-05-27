Cocktail
========

Cute abstract database for Erlang inspired by KVS.

Installing
----------

In rebar.config:

```Erlang
{cocktail, ".*", {git, "git://github.com/tapsters/cocktail", {tag, "master"}}}
```

Starting up
-----------

In order to start working with Cocktail you should call `ctail:init/0`,
but some backends require to make additional function calls.

For example builtin backend for Mnesia requires to call `ctail_mnesia:join/0` or
`ctail_mnesia:join/1`.

After performing all required calls you will have ready to use schema in your 
database (database, tables and indexes).

Example:

```Erlang
1> ctail:init().
ok

2> ctail_mnesia:join().
ok
```

Choosing backend
----------------

You can specify backend you want use in `sys.config`:

```Erlang
[
  {cocktail, [
    {backend, ctail_mnesia},
  ]}
]
```

Coctail uses Mnesia by default. 

List of available backends:
* Mnesia
* [MongoDB](https://github.com/tapsters/cocktail-mongo)

If you want create your own backend, you should implement `ctail_backend`
behaviour.

Working with schema
-------------------

Get tables list:

```Erlang
1> ctail:dir().
["feed","user"]
```

Destroy all tables:

```Erlang
1> ctail:destroy().
ok
```

Basic type for Cocktail is record. You should describe table for all your records.
To do this you should implement `ctail_schema` behaviour:

```Erlang
-module(user).
-behaviour(ctail_schema).

-include_lib("cocktail/include/ctail.hrl").

-export([meta/0]).

meta() ->
  #schema{name=myschema, tables=[
    #table{name=user, fields=record_info(fields, user), keys = [email]}
  ]}.
```

And then add your module to `sys.config`:

```Erlang
[
  {cocktail, [
    {schema, [user]},
  ]}
]
```

Backends implement two schema related functions: `create_table/1` and 
`add_table_index/2`.

Raw operations
--------------

Cocktail provides following functions to work with raw data:
* next_id/2
* put/1
* delete/2
* get/2
* index/3
* all/1
* count/1

When you persist new item to database you should specify id. Backends 
implement `next_id/2` to handle it. Example:

```Erlang
ctail:put(#user{id=ctail:next_id(user), firstName="Bob"}). 
```

Also you can specify increment but it's ignored by some backends:

```Erlang
ctail:put(#user{id=ctail:next_id(user, 1), firstName="Bob"}). 
```

Raw operations usage example:

```Erlang
1> ctail:put(#user{id=ctail:next_id(user), firstName="Bob", status=0}).
ok
2> ctail:put(#user{id=ctail:next_id(user), firstName="Fred", status=0}).
ok
3> ctail:put(#user{id=ctail:next_id(user), firstName="Pet", status=1}).
ok

4> JohnId = ctail:next_id(user).
4

5> ctail:put(#user{id=JohnId, firstName="John"}).
ok
6> ctail:get(user, JohnId).
{ok,#user{id = 4,firstName = "John",lastName = undefined,
                 username = undefined,status = undefined}}
7> ctail:delete(user, JohnId).
ok

8> ctail:index(user, status, 0).
[#user{id = 1,firstName = "Bob",lastName = undefined,
              username = undefined,status = 0}
 #user{id = 2,firstName = "Fred",lastName = undefined,
              username = undefined,status = 0}]

9> ctail:all(user).
[#user{id = 1,firstName = "Bob",lastName = undefined,
              username = undefined,status = 0}
 #user{id = 2,firstName = "Fred",lastName = undefined,
              username = undefined,status = 0}
 #user{id = 3,firstName = "Pet",lastName = undefined,
              username = undefined,status = 1}}]

10> ctail:count(user).
3
```

Chain operations
----------------

There are two domains provided by Cocktail to work with linked lists:
container and iterator.

Container holds list's head and count:

```Erlang
-define(CONTAINER, 
  id, 
  top, 
  count=0
).

-record(container, {?CONTAINER}).
-record(feed,      {?CONTAINER}).
```

You can store containers in you own table[s], but Coctail provides you
with builtin `feed` table.

Iterator is like superclass for your records. It defines base fields and 
adds some fields needed by chain operations:

```Erlang
-define(ITERATOR(ContainerName), 
  id, 
  version,
  container=ContainerName, 
  feed_id, 
  prev, 
  next
).

-record(iterator,  {?ITERATOR(undefined)}).
```

`#iterator.container` is name of the table where you want to store containers
for this record.

If you want to your record be ready to use in chain operations, you should
include ITERATOR macro as first item:

```Erlang

-record(message, {
  ?ITERATOR(feed),
  origin, 
  payload, 
  createdAt
}).
```

