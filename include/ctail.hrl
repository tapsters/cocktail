-ifndef(CTAIL_HRL).
-define(CTAIL_HRL, 1).

-record(schema, {
  name,
  tables=[]
}).

-record(table, {
  name,
  container=feed,
  fields=[],
  keys=[],
  options=[]
}).

-define(CONTAINER,
  id,
  top,
  bottom,
  count=0
).

-define(ITERATOR(ContainerName),
  id,
  version,
  container=ContainerName,
  feed_id,
  prev,
  next
).

-record(container,    {?CONTAINER}).
-record(feed,         {?CONTAINER}).
-record(iterator,     {?ITERATOR(undefined)}).

-endif.
