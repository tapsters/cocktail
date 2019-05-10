-module(ctail_backend).

-include("ctail.hrl").

-callback init() ->
  ok | {error, any()}.

-callback create_table(Table :: #table{}) ->
  ok | {error, any()}.

-callback add_table_index(Table :: atom(), Field :: atom()) ->
  ok | {error, any()}.

-callback dir() ->
  list(atom()).

-callback destroy() ->
  ok.

-callback put(Record :: tuple()) ->
  ok | {error, any()}.

-callback delete(Table :: atom(), Key :: any()) ->
  ok | {error, any()}.

-callback next_id() ->
  any().

-callback get(Table :: atom(), Key :: any()) ->
  {ok, any()}
  | {error, duplicated}
  | {error, not_found}.

-callback index(Table :: atom(), Key :: any(), Value :: any()) ->
  list(tuple()).

-callback all(Table :: atom()) ->
  list(tuple()).

-callback count(Table :: atom()) ->
  integer().
