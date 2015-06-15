-module(ctail).

-include("ctail.hrl").

-compile({no_auto_import, [put/2]}).

%% Raw ops
-export([init/0, init/1]).
-export([dir/0, dir/1]).
-export([destroy/0, destroy/1]).
-export([next_id/1, next_id/2, next_id/3]).
-export([put/1, put/2]).
-export([delete/2, delete/3]).
-export([get/2, get/3]).
-export([index/3, index/4]).
-export([all/1, all/2]).
-export([count/1, count/2]).

%% Chain ops
-export([create/2, create/3]).
-export([add/1, add/2]).
-export([link/1, link/2]).
-export([feed/3, feed/4]).
-export([entries/4, entries/5]).
-export([remove/2, remove/3]).

%% Table utils
-export([create_schema/0, create_schema/1, create_schema/2]).

%% Utils
-export([modules/0]).
-export([tables/0]).
-export([table/1]).
-export([containers/0]).
-export([config/1, config/2, config/3]).

-define(BACKEND, config(backend, ctail_mnesia)).

-type id() :: any().
-export_type([id/0]).

%% Raw ops

-spec init() -> ok.
init() -> 
  init(?BACKEND).

-spec init(Backend::module()) -> ok.
init(Backend) -> 
  Backend:init().

-spec dir() -> list(atom()).
dir() -> 
  dir(?BACKEND).

-spec dir(Backend::module()) -> list(atom()).
dir(Backend) -> 
  Backend:dir().

-spec destroy() -> ok.
destroy() -> destroy(?BACKEND).

-spec destroy(Backend::module()) -> ok.
destroy(Backend) -> 
  Backend:destroy().

-spec next_id(Table::atom()) -> id().
next_id(Table) -> 
  next_id(Table, 1, ?BACKEND).

-spec next_id(Table::atom(), Incr::integer()) -> id().
next_id(Table, Incr) -> 
  next_id(Table, Incr, ?BACKEND).

-spec next_id(Table::atom(), Incr::integer(), Backend::module()) -> id().
next_id(Table, Incr, Backend) -> 
  Backend:next_id(Table, Incr).

-spec put(Record::tuple()) -> ok | {error, _}.
put(Record) -> 
  put(Record, ?BACKEND).

-spec put(Record::tuple(), Backend::module()) -> ok | {error, _}.
put(Record, Backend) -> 
  Backend:put(Record).

-spec delete(Table::atom(), Key::id()) -> ok | {error, _}.
delete(Table, Key) -> 
  delete(Table, Key, ?BACKEND).

-spec delete(Table::atom(), Key::id(), Backend::module()) -> ok | {error, _}.
delete(Table, Key, Backend) -> 
  Backend:delete(Table, Key).

-spec get(Table::atom(), Key::id()) -> {ok, tuple()} | {error, _}.
get(Table, Key) -> 
  get(Table, Key, ?BACKEND).

-spec get(Table::atom(), Key::id(), Backend::module()) -> {ok, tuple()} | {error, _}.
get(Table, Key, Backend) -> 
  Backend:get(Table, Key).

-spec index(Table::atom(), Key::any(), Value::any()) -> list(tuple()).
index(Table, Key, Value) -> 
  index(Table, Key, Value, ?BACKEND).

-spec index(Table::atom(), Key::any(), Value::any(), Backend::module()) -> list(tuple()).
index(Table, Key, Value, Backend) -> 
  Backend:index(Table, Key, Value).

-spec all(Table::atom()) -> list(tuple()).
all(Table) -> 
  all(Table, ?BACKEND).

-spec all(Table::atom(), Backend::module()) -> list(tuple()) | [].
all(Table, Backend) -> 
  Backend:all(Table).

-spec count(Table::atom()) -> integer().
count(Table) -> 
  count(Table, ?BACKEND).

-spec count(Table::atom(), Backend::module()) -> integer().
count(Table, Backend) -> 
  Backend:count(Table).

%% Chain ops

-spec create(ContainerName::atom(), Id::id()) -> id().
create(ContainerName, Id) -> 
  create(ContainerName, Id, ?BACKEND).

-spec create(ContainerName::atom(), Id::id(), Backend::module()) -> id().
create(ContainerName, Id, Backend) ->
  Container = proplists:get_value(ContainerName, containers()),
  Instance = list_to_tuple([ContainerName |Container]),

  Top  = setelement(#container.id, Instance, Id),
  Top2 = setelement(#container.top, Top, undefined),
  Top3 = setelement(#container.count, Top2, 0),

  ok = put(Top3, Backend),
  Id.

-spec add(Record::tuple()) -> {ok, tuple()} | {error, _}.
add(Record) -> 
  add(Record, ?BACKEND).

-spec add(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
add(Record, Backend) when is_tuple(Record) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case get(Table, Id, Backend) of
    {error, _} ->
      ensure_link(Record, Backend);
    {ok, _} ->
      {error, exist}
  end.

-spec link(Record::tuple()) -> {ok, tuple()} | {error, _}.
link(Record) -> 
  link(Record, ?BACKEND).

-spec link(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
link(Record, Backend) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case get(Table, Id, Backend) of
    {ok, Record1} ->
      ensure_link(Record1, Backend);
    {error, not_found} ->
      {error, not_found}
  end.

-spec feed(Table::atom(), FeedId::any(), Count::integer()) -> none() | list(tuple()).
feed(Table, FeedId, Count) -> 
  feed(Table, FeedId, Count, ?BACKEND).

-spec feed(Table::atom(), FeedId::any(), Count::integer(), Backend::module()) -> 
        none() | list(tuple()).
feed(Table, FeedId, Count, Backend) ->
  case get(feed, FeedId) of
    {ok, Container} ->
      Start = element(#container.top, Container),
      entries(Table, Start, Count, #iterator.prev, Backend);
    {error, not_found} -> []
  end.

-spec entries(Table::atom(), Start::id(), Count::integer(), Direction::any()) -> 
        none() | list(tuple()).
entries(Table, Start, Count, Direction) -> 
  entries(Table, Start, Count, Direction, ?BACKEND).

-spec entries(Table::atom(), Start::id(), Count::integer(), Direction::any(), 
              Backend::module()) -> none() | list(tuple()).
entries(Table, Start, Count, Direction, Backend) ->
  Records = traversal(Table, Start, Count, Direction, Backend),
  case Direction of
    #iterator.next -> lists:reverse(Records);
    #iterator.prev -> Records
  end.

-spec remove(Table::atom(), Id::id()) -> ok | {error, _}.
remove(Table, Key) -> 
  remove(Table, Key, ?BACKEND).

-spec remove(Table::atom(), Id::id(), Backend::module()) -> ok | {error, _}.
remove(Table, Id, Backend) ->
  case Backend:get(Table, Id) of
    {error, not_found} ->
      {error, not_found};
    {ok, Record} ->
      Container = element(#iterator.container, Record),
      FeedId    = element(#iterator.feed_id, Record),

      case Backend:get(Container, FeedId) of
        {ok, Container1} ->
          relink(Container1, Record, Backend);
        _ ->
          skip
      end,

      Backend:delete(Table, Id)
  end.

%% Table utils

-spec create_schema() -> ok.
create_schema() -> 
  create_schema(?BACKEND), ok.

-spec create_schema(Backend::module()) -> ok.
create_schema(Backend) -> 
  [ create_schema(Module, Backend) || Module <- modules() ], ok.

-spec create_schema(Module::module(), Backend::module()) -> ok.
create_schema(Module, Backend) -> 
  [ create_table(Table, Backend) || Table <- (Module:meta())#schema.tables ], ok.

-spec create_table(Table::#table{}, Backend::module()) -> ok.
create_table(Table, Backend) ->
  Backend:create_table(Table),
  [ Backend:add_table_index(Table#table.name, Key) || Key <- Table#table.keys ],
  ok.

%% Utils

-spec modules() -> list(atom()).
modules() -> 
  config(schema, [])++[ctail_schema].

-spec tables() -> list(#table{}).
tables() -> 
  lists:flatten([ (Module:meta())#schema.tables || Module <- modules() ]).

-spec table(Name::atom()) -> #table{}.
table(Name) -> 
  lists:keyfind(Name, #table.name, tables()).

-spec containers() -> list({ atom(), list(atom()) }).
containers() ->
    lists:flatten([ [ {Table#table.name, Table#table.fields}
        || Table=#table{container=true} <- (Module:meta())#schema.tables ]
    || Module <- modules() ]).

-spec config(Key::atom()) -> any().
config(Key) -> 
  config(Key, undefined).

-spec config(Key::atom(), Default::any()) -> any().
config(Key, Default) -> 
  config(cocktail, Key, Default).

-spec config(App::atom(), Key::atom(), Default::any()) -> any().
config(App, Key, Default) ->
  case application:get_env(App, Key) of
    undefined -> Default;
    {ok, Value} -> Value
  end.

%% Internal

-spec ensure_link(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
ensure_link(Record, Backend) ->
  Table         = element(1, Record),
  Id            = element(#iterator.id, Record),
  ContainerName = element(#iterator.container, Record),
  FeedId        = case element(#iterator.feed_id, Record) of
                    undefined -> Table;
                    FeedId1 -> FeedId1
                  end,
  
  Container = case get(ContainerName, FeedId, Backend) of
                {ok, Result} ->
                  Result;
                {error, _} when FeedId /= undefined ->
                  ContainerFields = proplists:get_value(ContainerName, containers()),
                  Container1 = list_to_tuple([ContainerName|[undefined || _ <- ContainerFields]]),
                  Container2 = setelement(#container.id, Container1, FeedId),
                  Container3 = setelement(#container.count, Container2, 0),
                  Container3;
                _Error ->
                  error
              end,

  case Container of
    error ->
      {error, no_container};
    _ when element(#container.top, Container) == Id ->
      {error, just_added};
    _ ->
      Next = undefined,
      Prev = case element(#container.top, Container) of
               undefined ->
                 undefined;
               TopId ->
                 case get(Table, TopId, Backend) of
                   {error, _} ->
                     undefined;
                   {ok, Top} ->
                     NewTop = setelement(#iterator.next, Top, Id),
                     put(NewTop, Backend),
                     element(#iterator.id, Top)
                 end
             end,

      Container4 = setelement(#container.top, Container, Id),
      Count      = element(#container.count, Container),
      Container5 = setelement(#container.count, Container4, Count+1),

      put(Container5, Backend), %% Container

      Record1 = setelement(#iterator.next, Record, Next),
      Record2 = setelement(#iterator.prev, Record1, Prev),
      FeedId2 = element(#container.id, Container),
      Record3 = setelement(#iterator.feed_id, Record2, FeedId2),

      put(Record3, Backend), % Iterator

      {ok, Record3}
  end.

-spec relink(Container::tuple(), Record::tuple(), Backend::module()) -> ok | {error, _}.
relink(Container, Record, Backend) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),
  Next  = element(#iterator.next, Record),
  Prev  = element(#iterator.prev, Record),
  Top   = element(#container.top, Container),

  case get(Table, Prev, Backend) of
    {ok, Prev1} ->
      Prev2 = setelement(#iterator.next, Prev1, Next),
      Backend:put(Prev2);
    _ -> ok
  end,

  case get(Table, Next, Backend) of
    {ok, Next1} ->
      Next2 = setelement(#iterator.prev, Next1, Prev),
      Backend:put(Next2);
    _ -> ok
  end,

  Containter1 = case Top of
                  Id -> setelement(#container.top, Container, Prev);
                  _  -> Container
                end,
  Count = element(#container.count, Containter1),
  Containter2 = setelement(#container.count, Containter1, Count-1),

  Backend:put(Containter2).

-spec traversal(Table::atom(), Start::id(), Count::integer(), Direction::integer(),
    Backend::module()) -> list(tuple()).
traversal(Table, Start, Count, Direction, Backend) -> 
  iterate(Table, Start, Count, Direction, Backend, []).

-spec iterate(Table::atom(), Start::id(), Count::integer(), Direction::integer(),
    Backend::module(), Acc::list(tuple())) -> list(tuple()).
iterate(_Table,  undefined, _Count, _Direction, _Backend, Acc) -> Acc;
iterate(_Table, _Start,      0,     _Direction, _Backend, Acc) -> Acc;
iterate( Table,  Start,      Count,  Direction,  Backend, Acc) ->
  case Backend:get(Table, Start) of
    {ok, Record} ->
      Linked = element(Direction, Record),
      Count1 = case Count of
                 Count2 when is_integer(Count2) -> Count2 - 1;
                 _-> Count
               end,
      iterate(Table, Linked, Count1, Direction, Backend, [Record|Acc]);
    _Error ->
      Acc
  end.
