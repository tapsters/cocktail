-module(ctail_mnesia).
-behaviour(ctail_backend).
-behaviour(ctail_schema).

-include_lib("stdlib/include/qlc.hrl").
-include("ctail.hrl").
-include("ctail_mnesia.hrl").

%% Schema callbacks
-export([meta/0]).

%% Custom functions
-export([join/0, join/1, change_storage/2, exec/1]).

%% Backend callbacks
-export([init/0]).
-export([create_table/1, add_table_index/2, dir/0, destroy/0]).
-export([next_id/2, put/1, delete/2]).
-export([get/2, index/3, all/1, count/1]).

-type mnesia_strategy() :: ram_copies | disc_only_copies | disc_copies.

-spec init() -> ok | {error, any()}.
init() -> mnesia:start().

-spec create_table(Table::#table{}) -> ok | {error, any()}.
create_table(Table) ->
  Options = [{attributes, Table#table.fields}],
  Options2 = case proplists:lookup(copy_type, Table#table.options) of
               {copy_type, CopyType} -> [{CopyType, [node()]} | Options];
               _ -> Options
             end,
  case mnesia:create_table(Table#table.name, Options2) of
    {atomic, ok} -> ok;
    {aborted, Error} -> {error, Error}
  end.

-spec add_table_index(Table::atom(), Field::atom()) -> ok | {error, any()}.
add_table_index(Table, Field) ->
  case mnesia:add_table_index(Table, Field) of
    {atomic, ok} -> ok;
    {aborted, Error} -> {error, Error}
  end.

-spec dir() -> list(atom()).
dir() -> mnesia:system_info(local_tables).

-spec destroy() -> ok.
destroy() ->
  [ mnesia:delete_table(T) || T <- ctail:dir() ],
  mnesia:delete_schema([node()]),
  ok.

-spec put(Record::tuple() | list(tuple())) -> ok | {error, any()}.
put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).

-spec delete(Table::atom(), Key::ctail:id()) -> ok | {error, any()}.
delete(Table, Key) ->
  case mnesia:activity(context(), fun()-> mnesia:delete({Table, Key}) end) of
    {aborted, Reason} -> {error, Reason};
    {atomic, _Result} -> ok;
    ok -> ok
  end.

-spec next_id(Table::atom(), Incr::integer()) -> ctail:id().
next_id(Table, Incr) -> mnesia:dirty_update_counter({id_seq, Table}, Incr).

-spec get(Table::atom(), Key::ctail:id()) ->
  {ok, tuple()}
  | {error, duplicated}
  | {error, not_found}.
get(Table, Key) -> just_one(fun() -> mnesia:read(Table, Key) end).

-spec index(Table::atom(), Key::ctail:id(), Value::any()) -> list(tuple()).
index(Table, Key, Value) ->
  TableInfo = ctail:table(Table),
  Index = string:str(TableInfo#table.fields, [Key]),
  lists:flatten(many(fun() -> mnesia:index_read(Table, Value, Index+1) end)).

-spec all(Table::atom()) -> list(tuple()).
all(R) -> lists:flatten(many(fun() -> L = mnesia:all_keys(R), [mnesia:read({R, G}) || G <- L] end)).

-spec count(Table::atom()) -> integer().
count(Table) -> mnesia:table_info(Table, size).

-spec meta() -> #schema{name::ctail, tables::list(#table{})}.
meta() ->
  #schema{name=ctail, tables=[
    #table{name=id_seq, fields=record_info(fields, id_seq), keys=[thing]}
  ]}.

-spec join() -> ok | {timeout, list(any())} | {error, any()}.
join() ->
  mnesia:change_table_copy_type(schema, node(), ctail:config(mnesia_media, disc_copies)),
  mnesia:create_schema([node()]),

  ctail:create_schema(?MODULE),
  ctail:create_schema(?MODULE, ?MODULE),

  mnesia:wait_for_tables([ T#table.name || T <- ctail:tables()], infinity).

-spec join(Node::node()) -> list({atom(), any()}).
join(Node) ->
  mnesia:change_config(extra_db_nodes, [Node]),
  mnesia:change_table_copy_type(schema, node(), ctail:config(mnesia_media, disc_copies)),

  [{Tb, mnesia:add_table_copy(Tb, node(), Type)} ||
   {Tb, [{N, Type}]} <- [{T, mnesia:table_info(T, where_to_commit)} ||
                         T <- mnesia:system_info(tables)], Node==N].

-spec change_storage(Table::atom(), Type::mnesia_strategy()) -> {atomic, ok} | {aborted, any()}.
change_storage(Table, Type) -> mnesia:change_table_copy_type(Table, node(), Type).

-spec exec(Q::qlc:query_handle() | list()) -> any(). % ???
exec(Q) ->
  F = fun() -> qlc:e(Q) end,
  {atomic, Val} = mnesia:activity(context(), F),
  Val.

-spec context() -> any().
context() -> ctail:config(mnesia_context, async_dirty).

-spec many(Fun::fun()) -> {atomic, any()} | any().
many(Fun) ->
  case mnesia:activity(context(), Fun) of
    {atomic, R} -> R;
    X -> X
  end.

-spec void(Fun::fun()) -> ok | {error, any()} | any().
void(Fun) ->
  case mnesia:activity(context(), Fun) of
    {atomic, ok} -> ok;
    {aborted, Error} -> {error, Error};
    X -> X
  end.

-spec just_one(Fun::fun()) -> {error, not_found} | {error, duplicated} | {ok, any()} | any().
just_one(Fun) ->
  case mnesia:activity(context(), Fun) of
    {atomic, []} -> {error, not_found};
    {atomic, [R]} -> {ok, R};
    {atomic, [_|_]} -> {error, duplicated};
    [] -> {error, not_found};
    [R] -> {ok, R};
    [_|_] -> {error, duplicated};
    Error -> Error
  end.
