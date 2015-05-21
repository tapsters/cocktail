-module(ctail).

-include_lib("stdlib/include/qlc.hrl").
-include("ctail.hrl").

-export([init/0, init/1]).
-export([create_schema/0, create_schema/1]).

-export([modules/0]).
-export([tables/0]).
-export([table/1]).
-export([config/1, config/2]).

-define(BACKEND, ?MODULE:config(backend)).

init()                   -> init(?BACKEND).
start()                  -> start(?BACKEND).
stop()                   -> stop(?BACKEND).
dir()                    -> dir(?BACKEND).
destroy()                -> destroy(?BACKEND).
next_id(Table, Incr)     -> next_id(Table, Incr, ?BACKEND).
put(Table)               -> put(Table, ?BACKEND).
delete(Table, Key)       -> delete(Table, Key, ?BACKEND).
get(Table, Key)          -> get(Table, Key, ?BACKEND).
index(Table, Key, Value) -> index(Table, K,V, ?BACKEND).
all(Table)               -> all(Table, ?BACKEND).
count(Table)             -> count(Table, ?BACKEND).

init(Backend)               -> Backend:init().
start(Backend)              -> Backend:start().
stop(Backend)               -> Backend:stop().
dir(Backend)                -> Backend:dir().
destroy(Backend)            -> Backend:destroy().
put(Record, Backend)        -> Backend:put(Record).
delete(Table, Key, Backend) -> Backend:delete(Table, Key).

add(Table)            -> add(Table, ?BACKEND).
link(Table)           -> link(Table, ?BACKEND).
entries(A, B, C)      -> entries(A, B, C, ?BACKEND).
traversal(T, S, C, D) -> traversal(T, S, C, D, ?BACKEND).
remove(Table, Key)    -> remove(Table, Key, ?BACKEND).

% Implementation

modules() -> 
  ?MODULE:config(schema).

tables() ->
  lists:flatten([ (Module:meta())#schema.tables || Module <- modules() ]).

table(Name) -> 
  lists:keyfind(Name, #table.name, tables()).

containers() ->
    lists:flatten([ [ {Table#table.name, Table#table.fields}
        || Table=#table{container=true} <- (Module:meta())#schema.tables ]
    || Module <- modules() ]).

create_schema()        -> init(?BACKEND).
create_schema(Backend) ->
  [ ?MODULE:create_schema(Module, Backend) || Module <- ctail:modules() ].

create_schema(Module, Backend) ->
  [ create_table(Table, Backend) || Table <- (Module:meta())#schema.tables ].

create_table(Table, Backend) ->
  Backend:create_table(Table#table.name),
  [ Backend:add_table_index(Table#table.name, Key) || Key <- Table#table.keys ].

create(ContainerName) -> 
  create(ContainerName, ?MODULE:next_id(atom_to_list(ContainerName), 1), ?BACKEND).

%% WIP

create(ContainerName, Id, Backend) ->
    Instance = list_to_tuple([ContainerName|proplists:get_value(ContainerName, ?MODULE:containers())]),
    Top  = setelement(#container.id, Instance, Id),
    Top2 = setelement(#container.top, Top, undefined),
    Top3 = setelement(#container.count, Top2, 0),
    ok = ?MODULE:put(Top3, Backend),
    Id.

ensure_link(Record, #kvs{mod=_Store}=Backend) ->
    Id    = element(2,Record),
    Type  = table_type(element(1,Record)),
    CName = element(#iterator.container, Record),
    Cid   = table_type(case element(#iterator.feed_id, Record) of
               undefined -> element(1,Record);
                     Fid -> Fid end),

    Container = case ?MODULE:get(CName, Cid, Backend) of
        {ok,Res} -> Res;
        {error, _} when Cid /= undefined ->
                NC = setelement(#container.id,
                      list_to_tuple([CName|
                            proplists:get_value(CName, ?MODULE:containers())]), Cid),
                NC1 = setelement(#container.count, NC, 0),
                NC1;
        _Error -> error end,

    case Container of
              error -> {error, no_container};
        _ when element(#container.top,Container) == Id -> {error,just_added};
                  _ ->
                       Next = undefined,
                       Prev = case element(#container.top, Container) of
                                   undefined -> undefined;
                                   Tid -> case ?MODULE:get(Type, Tid, Backend) of
                                              {error, _} -> undefined;
                                                       {ok, Top} ->
                                        NewTop = setelement(#iterator.next, Top, Id),
                                        ?MODULE:put(NewTop, Backend),
                                        element(#iterator.id, NewTop) end end,

                       C1 = setelement(#container.top, Container, Id),
                       C2 = setelement(#container.count, C1,
                                element(#container.count, Container)+1),

                       ?MODULE:put(C2, Backend), % Container

                       R  = setelement(#iterator.feeds, Record,
                            [ case F1 of
                                {FN, Fd} -> {FN, Fd};
                                _-> {F1, ?MODULE:create(CName,{F1,element(#iterator.id,Record)},Backend)}
                              end || F1 <- element(#iterator.feeds, Record)]),

                       R1 = setelement(#iterator.next,    R,  Next),
                       R2 = setelement(#iterator.prev,    R1, Prev),
                       R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                       ?MODULE:put(R3, Backend), % Iterator

                    {ok, R3}
            end.

link(Record,#kvs{mod=_Store}=Backend) ->
    Id = element(#iterator.id, Record),
    case ?MODULE:get(element(1,Record), Id, Backend) of
              {ok, Exists} -> ensure_link(Exists, Backend);
        {error, not_found} -> {error, not_found} end.

add(Record, #kvs{mod=_Store}=Backend) when is_tuple(Record) ->
    Id = element(#iterator.id, Record),
    case ?MODULE:get(element(1,Record), Id, Backend) of
                {error, _} -> ensure_link(Record, Backend);
         {aborted, Reason} -> {aborted, Reason};
                   {ok, _} -> {error, exist} end.

reverse(#iterator.prev) -> #iterator.next;
reverse(#iterator.next) -> #iterator.prev.

relink(Container, E, Backend) ->
    Id   = element(#iterator.id, E),
    Next = element(#iterator.next, E),
    Prev = element(#iterator.prev, E),
    Top  = element(#container.top, Container),
    case ?MODULE:get(element(1,E), Prev, Backend) of
         {ok, PE} -> ?MODULE:put(setelement(#iterator.next, PE, Next), Backend);
         _ -> ok end,
    case ?MODULE:get(element(1,E), Next, Backend) of
         {ok, NE} -> ?MODULE:put(setelement(#iterator.prev, NE, Prev), Backend);
                _ -> ok end,
    C  = case Top of
               Id -> setelement(#container.top, Container, Prev);
                _ -> Container end,
    ?MODULE:put(setelement(#container.count,C,element(#container.count,C)-1), Backend).

remove(Record,Id,#kvs{mod=Mod}=Backend) ->
    case Mod:get(Record,Id) of
         {error, not_found} -> ?MODULE:error(?MODULE,"Can't remove ~p~n",[{Record,Id}]);
                     {ok,R} -> do_remove(R,Backend) end.

do_remove(E,#kvs{mod=Mod}=Backend) ->
    case Mod:get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
         {ok, Container} -> relink(Container,E,Backend);
                       _ -> skip end,
    ?MODULE:delete(element(1,E),element(2,E), Backend).

traversal(Table, Start, Count, Direction, Backend)->
    fold(fun(A,Acc) -> [A|Acc] end,[],Table,Start,Count,Direction,Backend).

fold(___,___,_,undefined,_,_,_) -> [];
fold(___,Acc,_,_,0,_,_) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Backend) ->
    RecordType = table_type(Table),
    case ?MODULE:get(RecordType, Start, Backend) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Backend);
           Error -> ?MODULE:error(?MODULE,"Error: ~p~n",[Error]), Acc end.

entries({error,_},_,_,_)      -> [];
entries({ok,Container},N,C,Backend) -> entries(Container,N,C,Backend);
entries(T,N,C,Backend)              -> traversal(N,element(#container.top,T),C,#iterator.prev,Backend).
entries(N, Start, Count, Direction, Backend) ->
    E = traversal(N, Start, Count, Direction, Backend),
    case Direction of #iterator.next -> lists:reverse(E);
                      #iterator.prev -> E end.

add_seq_ids() ->
    Init = fun(Key) ->
           case ?MODULE:get(id_seq, Key) of
                {error, _} -> {Key,?MODULE:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- containers() ].

table_type(user2) -> user;
table_type(A) -> A.

range(RecordName,Id) -> Ranges = ?MODULE:config(RecordName), find(Ranges,RecordName,Id).

find([],_,_Id) -> [];
find([Range|T],RecordName,Id) ->
     case lookup(Range,Id) of
          [] -> find(T,RecordName,Id);
          Name -> Name end.

lookup(#interval{left=Left,right=Right,name=Name},Id) when Id =< Right, Id >= Left -> Name;
lookup(#interval{},_Id) -> [].

get(RecordName, Key, #kvs{mod=Mod}) ->
    case range(RecordName,Key) of
         [] -> Mod:get(RecordName, Key);
         Name ->  Mod:get(Name, Key) end.

count(RecordName,#kvs{mod=DBA}) -> Backend:count(RecordName).
all(RecordName,#kvs{mod=DBA}) -> Backend:all(RecordName).
index(RecordName, Key, Value,#kvs{mod=DBA}) -> Backend:index(RecordName, Key, Value).
next_id(RecordName, Incr,#kvs{mod=DBA}) -> Backend:next_id(RecordName, Incr).

config(Key)          -> config(Key, undefined).
config(Key, Default) -> 
  case application:get_env(ctail, Key) of
    undefined -> Default;
    {ok, V} -> V 
  end.

