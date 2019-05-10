-module(ctail).
-include("ctail.hrl").
-compile({no_auto_import, [put/2]}).

%% Raw ops
-export([init/0, init/1]).
-export([dir/0, dir/1]).
-export([destroy/0, destroy/1]).
-export([next_id/0, next_id/1]).
-export([put/1, put/2]).
-export([delete/2, delete/3]).
-export([get/2, get/3]).
-export([index/3, index/4]).
-export([all/1, all/2]).
-export([count/1, count/2]).

%% Table utils
-export([create_schema/0, create_schema/1, create_schema/2]).

%% Utils
-export([backend/0]).
-export([modules/0]).
-export([tables/0]).
-export([table/1]).
-export([containers/0]).
-export([config/1, config/2, config/3]).

-type id() :: any().
-export_type([id/0]).

%% Raw ops

-spec init() -> ok.
init() -> 
  init(backend()).

-spec init(Backend::module()) -> ok.
init(Backend) -> 
  Backend:init().

-spec dir() -> list(atom()).
dir() -> 
  dir(backend()).

-spec dir(Backend::module()) -> list(atom()).
dir(Backend) -> 
  Backend:dir().

-spec destroy() -> ok.
destroy() -> destroy(backend()).

-spec destroy(Backend::module()) -> ok.
destroy(Backend) -> 
  Backend:destroy().

-spec next_id() -> id().
next_id() -> 
  next_id(backend()).

-spec next_id(Backend::module()) -> id().
next_id(Backend) -> 
  Backend:next_id().

-spec put(Record::tuple()) -> ok | {error, _}.
put(Record) -> 
  put(Record, backend()).

-spec put(Record::tuple(), Backend::module()) -> ok | {error, _}.
put(Record, Backend) -> 
  Backend:put(Record).

-spec delete(Table::atom(), Key::id()) -> ok | {error, _}.
delete(Table, Key) -> 
  delete(Table, Key, backend()).

-spec delete(Table::atom(), Key::id(), Backend::module()) -> ok | {error, _}.
delete(Table, Key, Backend) -> 
  Backend:delete(Table, Key).

-spec get(Table::atom(), Key::id()) -> {ok, tuple()} | {error, _}.
get(Table, Key) -> 
  get(Table, Key, backend()).

-spec get(Table::atom(), Key::id(), Backend::module()) -> {ok, tuple()} | {error, _}.
get(Table, Key, Backend) -> 
  Backend:get(Table, Key).

-spec index(Table::atom(), Key::any(), Value::any()) -> list(tuple()).
index(Table, Key, Value) -> 
  index(Table, Key, Value, backend()).

-spec index(Table::atom(), Key::any(), Value::any(), Backend::module()) -> list(tuple()).
index(Table, Key, Value, Backend) -> 
  Backend:index(Table, Key, Value).

-spec all(Table::atom()) -> list(tuple()).
all(Table) -> 
  all(Table, backend()).

-spec all(Table::atom(), Backend::module()) -> list(tuple()) | [].
all(Table, Backend) -> 
  Backend:all(Table).

-spec count(Table::atom()) -> integer().
count(Table) -> 
  count(Table, backend()).

-spec count(Table::atom(), Backend::module()) -> integer().
count(Table, Backend) -> 
  Backend:count(Table).

%% Table utils

-spec create_schema() -> ok.
create_schema() -> 
  create_schema(backend()), ok.

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

backend()->
  config(backend, ctail_mnesia).

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
