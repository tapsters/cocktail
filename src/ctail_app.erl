-module(ctail_app).

-behaviour(application).
-behaviour(supervisor).

-export([start/2, stop/1]).
-export([init/1]).

start(_,_) ->
  supervisor:start_link({local, ctail_sup}, ?MODULE, []).

stop(_) -> 
  ok.

init([]) ->
  {ok, {{one_for_one, 5, 10}, []}}.