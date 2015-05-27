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

init()                   -> init(?BACKEND).
dir()                    -> dir(?BACKEND).
destroy()                -> destroy(?BACKEND).
next_id(Table)           -> next_id(Table, 1, ?BACKEND).
next_id(Table, Incr)     -> next_id(Table, Incr, ?BACKEND).
put(Record)              -> put(Record, ?BACKEND).
delete(Table, Key)       -> delete(Table, Key, ?BACKEND).
get(Table, Key)          -> get(Table, Key, ?BACKEND).
index(Table, Key, Value) -> index(Table, Key, Value, ?BACKEND).
all(Table)               -> all(Table, ?BACKEND).
count(Table)             -> count(Table, ?BACKEND).

create(Name, Id)                        -> create(Name, Id, ?BACKEND).
add(Record)                             -> add(Record, ?BACKEND).
link(Record)                            -> link(Record, ?BACKEND).
feed(FeedId, Table, Count)              -> feed(FeedId, Table, Count, ?BACKEND).
entries(Table, Start, Count, Direction) -> entries(Table, Start, Count, Direction, ?BACKEND).
remove(Table, Key)                      -> remove(Table, Key, ?BACKEND).

init(Backend)                     -> Backend:init().
dir(Backend)                      -> Backend:dir().
destroy(Backend)                  -> Backend:destroy().
put(Record, Backend)              -> Backend:put(Record).
delete(Table, Key, Backend)       -> Backend:delete(Table, Key).
get(Table, Key, Backend)          -> Backend:get(Table, Key).
count(Table, Backend)             -> Backend:count(Table).
all(Table, Backend)               -> Backend:all(Table).
index(Table, Key, Value, Backend) -> Backend:index(Table, Key, Value).
next_id(Table, Incr, Backend)     -> Backend:next_id(Table, Incr).

modules() -> 
  config(schema, [])++[ctail_schema].

tables() ->
  lists:flatten([ (Module:meta())#schema.tables || Module <- modules() ]).

table(Name) -> 
  lists:keyfind(Name, #table.name, tables()).

containers() ->
    lists:flatten([ [ {Table#table.name, Table#table.fields}
        || Table=#table{container=true} <- (Module:meta())#schema.tables ]
    || Module <- modules() ]).

create_schema() -> 
  create_schema(?BACKEND).

create_schema(Backend) ->
  [ create_schema(Module, Backend) || Module <- modules() ].

create_schema(Module, Backend) ->
  [ create_table(Table, Backend) || Table <- (Module:meta())#schema.tables ].

create_table(Table, Backend) ->
  Backend:create_table(Table),
  [ Backend:add_table_index(Table#table.name, Key) || Key <- Table#table.keys ].

create(Name, Id, Backend) ->
  Container = proplists:get_value(Name, containers()),
  Instance = list_to_tuple([Name|Container]),
  
  Top  = setelement(#container.id, Instance, Id),
  Top2 = setelement(#container.top, Top, undefined),
  Top3 = setelement(#container.count, Top2, 0),

  ok = put(Top3, Backend), 
  Id.

ensure_link(Record, Backend) ->
  Table         = element(1, Record),
  Id            = element(#iterator.id, Record),
  ContainerName = element(#iterator.container, Record),
  ContainerId   = case element(#iterator.feed_id, Record) of
                    undefined -> Table;
                    FeedId -> FeedId
                  end,

  Container = case get(ContainerName, ContainerId, Backend) of
                {ok, Result} -> 
                  Result;
                {error, _} when ContainerId /= undefined ->
                  ContainerInfo = proplists:get_value(ContainerName, containers()),
                  Container1 = list_to_tuple([ContainerName|ContainerInfo]),
                  Container2 = setelement(#container.id, Container1, ContainerId),
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
      FeedId1 = element(#container.id, Container),
      Record3 = setelement(#iterator.feed_id, Record2, FeedId1), 

      put(Record3, Backend), % Iterator
      
      {ok, Record3}
  end.

link(Record, Backend) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case get(Table, Id, Backend) of
    {ok, Exists} -> 
      ensure_link(Exists, Backend);
    {error, not_found} -> 
      {error, not_found} 
  end.

add(Record, Backend) when is_tuple(Record) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case get(Table, Id, Backend) of
    {error, _} -> 
      ensure_link(Record, Backend);
    {aborted, Reason} -> 
      {aborted, Reason};
    {ok, _} -> 
      {error, exist} 
  end.

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

remove(Table, Id, Backend) ->
  case Backend:get(Table, Id) of
    {error, not_found} -> 
      {error, not_found};
    {ok, Record} -> 
      do_remove(Record, Backend)
  end.

do_remove(Record, Backend) ->
  Table     = element(1, Record),
  Id        = element(#iterator.id, Record),
  Container = element(#iterator.container, Record),
  FeedId    = element(#iterator.feed_id, Record),
  
  case Backend:get(Container, FeedId) of
    {ok, Container1} -> 
      relink(Container1, Record, Backend);
    _ -> 
      skip 
  end,

  Backend:delete(Table, Id).

traversal(Table, Start, Count, Direction, Backend) ->
  iterate(Table, Start, Count, Direction, Backend, []).

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

feed(Table, FeedId, Count, Backend) -> 
  {ok, Container} = ?MODULE:get(feed, FeedId),
  Start = element(#container.top, Container),
  entries(Table, Start, Count, #iterator.prev, Backend).

entries(Table, Start, Count, Direction, Backend) ->
  Records = traversal(Table, Start, Count, Direction, Backend),
  case Direction of 
    #iterator.next -> lists:reverse(Records);
    #iterator.prev -> Records 
  end.

config(Key) -> 
  config(Key, undefined).
config(Key, Default) -> 
  config(cocktail, Key, Default).
config(App, Key, Default) -> 
  case application:get_env(App, Key) of
    undefined -> Default;
    {ok, Value} -> Value
  end.

