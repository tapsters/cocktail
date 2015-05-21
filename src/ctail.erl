-module(ctail).

-include("ctail.hrl").

-compile({no_auto_import, [put/2]}).

%% Raw ops
-export([init/0, init/1]).
-export([dir/0, dir/1]).
-export([destroy/0, destroy/1]).
-export([next_id/2, next_id/3]).
-export([put/1, put/2]).
-export([delete/2, delete/3]).
-export([get/2, get/3]).
-export([index/3, index/4]).
-export([all/1, all/2]).
-export([count/1, count/2]).

%% Chain ops
-export([create/1, create/2, create/3]).
-export([add/1, add/2]).
-export([link/1, link/2]).
-export([entries/3, entries/4]).
-export([entries2/4, entries2/5]).
-export([traversal/4, traversal/5]).
-export([remove/2, remove/3]).

%% Table utils
-export([create_schema/0, create_schema/1]).
-export([add_seq_ids/0]).

%% Utils
-export([modules/0]).
-export([tables/0]).
-export([table/1]).
-export([containers/0]).
-export([config/1, config/2]).

-define(BACKEND, config(backend)).

init()                   -> init(?BACKEND).
dir()                    -> dir(?BACKEND).
destroy()                -> destroy(?BACKEND).
next_id(Table, Incr)     -> next_id(Table, Incr, ?BACKEND).
put(Table)               -> put(Table, ?BACKEND).
delete(Table, Key)       -> delete(Table, Key, ?BACKEND).
get(Table, Key)          -> get(Table, Key, ?BACKEND).
index(Table, Key, Value) -> index(Table, Key, Value, ?BACKEND).
all(Table)               -> all(Table, ?BACKEND).
count(Table)             -> count(Table, ?BACKEND).

add(Table)                                -> add(Table, ?BACKEND).
link(Table)                               -> link(Table, ?BACKEND).
entries(Container, Table, Count)          -> entries(Container, Table, Count, ?BACKEND).
entries2(Table, Start, Count, Direction)  -> entries2(Table, Start, Count, Direction, ?BACKEND).
traversal(Table, Start, Count, Direction) -> traversal(Table, Start, Count, Direction, ?BACKEND).
remove(Table, Key)                        -> remove(Table, Key, ?BACKEND).

init(Backend)                     -> Backend:init().
dir(Backend)                      -> Backend:dir().
destroy(Backend)                  -> Backend:destroy().
put(Record, Backend)              -> Backend:put(Record).
delete(Table, Key, Backend)       -> Backend:delete(Table, Key).
count(Table, Backend)             -> Backend:count(Table).
all(Table, Backend)               -> Backend:all(Table).
index(Table, Key, Value, Backend) -> Backend:index(Table, Key, Value).
next_id(Table, Incr, Backend)     -> Backend:next_id(Table, Incr).

modules() -> 
  config(schema).

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
  Backend:create_table(Table#table.name),
  [ Backend:add_table_index(Table#table.name, Key) || Key <- Table#table.keys ].

create(ContainerName) -> 
  create(ContainerName, next_id(atom_to_list(ContainerName), 1)).

create(ContainerName, Backend) ->
  create(ContainerName, next_id(atom_to_list(ContainerName), 1), Backend).

create(ContainerName, Id, Backend) ->
  Container = proplists:get_value(ContainerName, containers()),
  Instance = list_to_tuple([ContainerName|Container]),
  
  Top  = setelement(#container.id, Instance, Id),
  Top2 = setelement(#container.top, Top, undefined),
  Top3 = setelement(#container.count, Top2, 0),

  ok = put(Top3, Backend), 
  Id.

ensure_link(Record, Backend) ->
  Type          = element(1, Record),
  Id            = element(2, Record),
  ContainerName = element(#iterator.container, Record),
  ContainerId   = case element(#iterator.feed_id, Record) of
                    undefined -> 
                      element(1, Record);
                    Fid -> Fid 
                  end,
  
  Container = case get(ContainerName, ContainerId, Backend) of
                {ok, Result} -> 
                  Result;
                {error, _} when ContainerId /= undefined ->
                  ContainerInfo = proplists:get_value(ContainerName, containers()),
                  Container1 = list_to_tuple([ContainerName|ContainerInfo]),
                  Container2 = Container1#container{id=ContainerId, count=0},
                  Container2;
                _Error -> error 
              end,
  
  case Container of
    error -> 
      {error, no_container};
    _ when Container#container.top == Id -> 
      {error, just_added};
    _ ->
      Next = undefined,
      Prev = case Container#container.top of
               undefined -> undefined;
               TopId -> 
                 case get(Type, TopId, Backend) of
                   {error, _} -> 
                     undefined;
                   {ok, Top} ->
                     NewTop = setelement(#iterator.next, Top, Id),
                     put(NewTop, Backend),
                     NewTop#iterator.id
                 end 
             end,
      
      Containter2 = Container#container{top=Id, count=Container#container.count+1},
      
      put(Containter2, Backend), %% Container
      
      Feeds = 
        [ case Field of
            {FN, Fd} -> 
              {FN, Fd};
            _ -> 
              {Field, create(ContainerName, {Field, Record#iterator.id}, Backend)}
          end || Field <- Record#iterator.feeds ],

      Record1 = Record#iterator{feeds=Feeds, 
                                next=Next, 
                                prev=Prev, 
                                feed_id=Container#container.id},
      
      put(Record1, Backend), % Iterator
      
      {ok, Record1}
  end.

link(Record, Backend) ->
  Table = element(1, Record),
  Id    = Record#iterator.id,

  case get(Table, Id, Backend) of
    {ok, Exists} -> 
      ensure_link(Exists, Backend);
    {error, not_found} -> 
      {error, not_found} 
  end.

add(Record, Backend) when is_tuple(Record) ->
  Table = element(1, Record),
  Id    = Record#iterator.id,

  case get(Table, Id, Backend) of
    {error, _} -> 
      ensure_link(Record, Backend);
    {aborted, Reason} -> 
      {aborted, Reason};
    {ok, _} -> 
      {error, exist} 
  end.

relink(Container, Iterator, Backend) ->
  Id   = Iterator#iterator.id,
  Next = Iterator#iterator.next,
  Prev = Iterator#iterator.prev,
  Top  = Container#container.top,
  
  case get(Id, Prev, Backend) of
    {ok, Prev1} -> Backend:put(Prev1#iterator{next=Next}, Backend);
    _ -> ok 
  end,

  case get(Id, Next, Backend) of
    {ok, Next1} -> Backend:put(Next1#iterator{prev=Prev}, Backend);
    _ -> ok 
  end,
  
  Containter1 = case Top of
                  Id -> Container#container{top=Prev};
                  _ -> Container
                end,
  Containter2 = Containter1#container{count=Containter1#container.count-1},
  Backend:put(Containter2, Backend).

remove(Record, Id, Backend) ->
  case Backend:get(Record, Id) of
    {error, not_found} -> 
      {error, not_found};
    {ok, Record1} -> 
      do_remove(Record1, Backend)
  end.

do_remove(Record, Backend) ->
  case Backend:get(Record#iterator.container, Record#iterator.feed_id) of
    {ok, Container} -> 
      relink(Container, Record, Backend);
    _ -> 
      skip 
  end,
  Table = element(1, Record),
  Id    = element(2, Record),
  Backend:delete(Table, Id).

traversal(Table, Start, Count, Direction, Backend) ->
  fold(fun(A, Acc) -> [A|Acc] end, [], Table, Start, Count, Direction, Backend).

fold(_,_,_,undefined,_,_,_)                             -> [];
fold(_,Acc,_,_,0,_,_)                                   -> Acc;
fold(Fun, Acc, Table, Start, Count, Direction, Backend) ->
  case Backend:get(Table, Start) of
    {ok, R} -> 
      Prev = element(Direction, R),
      Count1 = case Count of 
                 C when is_integer(C) -> C - 1; 
                 _-> Count 
               end,
      fold(Fun, Fun(R, Acc), Table, Prev, Count1, Direction, Backend);
    _Error -> 
      Acc 
  end.

entries({error, _} ,_ ,_ ,_) -> [];
entries({ok, Container}, Table, Count, Backend) -> 
  entries(Container, Table, Count, Backend);
entries(Container, Table, Count, Backend) -> 
  traversal(Table, Container#container.top, Count, #iterator.prev, Backend).

entries2(Table, Start, Count, Direction, Backend) ->
  E = traversal(Table, Start, Count, Direction, Backend),
  case Direction of 
    #iterator.next -> lists:reverse(E);
    #iterator.prev -> E 
  end.

add_seq_ids() ->
  Init = fun(Key) ->
           case get(id_seq, Key) of
             {error, _} -> 
               {Key, put(#id_seq{thing=Key, id=0})};
             {ok, _} -> 
               {Key, skip} 
           end 
         end,
  [ Init(atom_to_list(Name)) || {Name, _Fields} <- containers() ].

range(Table, Id) -> 
  Ranges = config(Table), 
  find(Ranges, Table, Id).

find([], _, _Id) -> [];
find([Range|T], Table, Id) ->
  case lookup(Range, Id) of
    [] -> find(T, Table,Id);
    Name -> Name 
  end.

lookup(#interval{left=Left, right=Right, name=Name}, Id) when Id =< Right, Id >= Left -> Name;
lookup(#interval{}, _Id) -> [].

get(Table, Key, Backend) ->
  case range(Table, Key) of
    [] -> 
      Backend:get(Table, Key);
    Name -> 
      Backend:get(Name, Key) 
  end.

config(Key) -> 
  config(Key, undefined).
config(Key, Default) -> 
  case application:get_env(ctail, Key) of
    undefined -> Default;
    {ok, V} -> V 
  end.

