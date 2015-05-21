-module(ctail_feed).

-include("ctail.hrl").

meta() ->
  #schema{name=ctail, tables=[
    #table{name=feed, container=true, fields=record_info(fields, feed)}
  ]}.
