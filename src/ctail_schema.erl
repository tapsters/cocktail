-module(ctail_schema).

-include("ctail.hrl").

-export([meta/0]).

meta() ->
  #schema{name=ctail, tables=[
    #table{name=feed, container=true, fields=record_info(fields, feed)}
  ]}.
