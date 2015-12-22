-module(ctail_feed).
-include("ctail.hrl").

-export([add/1, add/2]).
-export([add_bottom/1, add_bottom/2]).
-export([get/3, get/4, get/5, get/6]).
-export([get_bottom/3, get_bottom/4, get_bottom/5, get_bottom/6]).
-export([remove/2, remove/3]).

-export([create/2, create/3]).
-export([link/1, link/2]).

%% API

-spec add(Record::tuple()) -> {ok, tuple()} | {error, _}.
add(Record) -> 
  add(Record, ctail:backend()).

-spec add(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
add(Record, Backend) when is_tuple(Record) ->
  add(Record, #iterator.prev, Backend).

-spec add_bottom(Record::tuple()) -> {ok, tuple()} | {error, _}.
add_bottom(Record) -> 
  add_bottom(Record, ctail:backend()).

-spec add_bottom(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
add_bottom(Record, Backend) when is_tuple(Record) ->
  add(Record, #iterator.next, Backend).

add(Record, Direction, Backend) when is_tuple(Record) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case Backend:get(Table, Id) of
    {error, _} ->
      ensure_link(Record, Direction, Backend);
    {ok, _} ->
      {error, exist}
  end.

-spec get(Table::atom(), FeedId::any(), Count::integer()) -> none() | list(tuple()).
get(Table, FeedId, Count) ->
  get(Table, FeedId, Count, ctail:backend()).

-spec get(Table::atom(), FeedId::any(), Count::integer(), Backend::module()) -> 
        none() | list(tuple()).
get(Table, FeedId, Count, Backend) ->
  get(Table, FeedId, undefined, undefined, Count, Backend).

-spec get(Table::atom(), FeedId::any(), StartId::ctail:id(), StopId::ctail:id(),
          Count::integer()) -> none() | list(tuple()).
get(Table, FeedId, StartId, StopId, Count) ->
  get(Table, FeedId, StartId, StopId, Count, ctail:backend()).

-spec get(Table::atom(), FeedId::any(), StartId::ctail:id(), StopId::ctail:id(),
          Count::integer(), Backend::module()) -> none() | list(tuple()).
get(Table, FeedId, StartId, StopId, Count, Backend) ->
  entries(Table, FeedId, StartId, StopId, Count, #iterator.prev, Backend).

-spec get_bottom(Table::atom(), FeedId::any(), Count::integer()) -> none() | list(tuple()).
get_bottom(Table, FeedId, Count) ->
  get_bottom(Table, FeedId, Count, ctail:backend()).

-spec get_bottom(Table::atom(), FeedId::any(), Count::integer(), Backend::module()) -> 
        none() | list(tuple()).
get_bottom(Table, FeedId, Count, Backend) ->
  get_bottom(Table, FeedId, undefined, undefined, Count, Backend).

-spec get_bottom(Table::atom(), FeedId::any(), StartId::ctail:id(), StopId::ctail:id(),
          Count::integer()) -> none() | list(tuple()).
get_bottom(Table, FeedId, StartId, StopId, Count) ->
  get_bottom(Table, FeedId, StartId, StopId, Count, ctail:backend()).

-spec get_bottom(Table::atom(), FeedId::any(), StartId::ctail:id(), StopId::ctail:id(),
          Count::integer(), Backend::module()) -> none() | list(tuple()).
get_bottom(Table, FeedId, StartId, StopId, Count, Backend) ->
  entries(Table, FeedId, StartId, StopId, Count, #iterator.next, Backend).

-spec remove(Table::atom(), Id::ctail:id()) -> ok | {error, _}.
remove(Table, Key) -> 
  remove(Table, Key, ctail:backend()).

-spec remove(Table::atom(), Id::ctail:id(), Backend::module()) -> ok | {error, _}.
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

-spec create(ContainerName::atom(), Id::ctail:id()) -> ctail:id().
create(ContainerName, Id) -> 
  create(ContainerName, Id, ctail:backend()).

-spec create(ContainerName::atom(), Id::ctail:id(), Backend::module()) -> ctail:id().
create(ContainerName, Id, Backend) ->
  Container = proplists:get_value(ContainerName, ctail:containers()),
  Instance = list_to_tuple([ContainerName |Container]),

  Top  = setelement(#container.id, Instance, Id),
  Top2 = setelement(#container.top, Top, undefined),
  Top3 = setelement(#container.count, Top2, 0),

  ok = Backend:put(Top3),
  Id.

-spec link(Record::tuple()) -> {ok, tuple()} | {error, _}.
link(Record) -> 
  link(Record, ctail:backend()).

-spec link(Record::tuple(), Backend::module()) -> {ok, tuple()} | {error, _}.
link(Record, Backend) ->
  Table = element(1, Record),
  Id    = element(#iterator.id, Record),

  case get(Table, Id, Backend) of
    {ok, Record1} ->
      ensure_link(Record1, #iterator.prev, Backend);
    {error, not_found} ->
      {error, not_found}
  end.

%% Internal

-spec ensure_link(Record::tuple(), Direction::integer(), Backend::module()) -> 
        {ok, tuple()} | {error, _}.
ensure_link(Record, Direction, Backend) ->
  Table         = element(1, Record),
  Id            = element(#iterator.id, Record),
  ContainerName = element(#iterator.container, Record),
  FeedId        = case element(#iterator.feed_id, Record) of
                    undefined -> Table;
                    FeedId1 -> FeedId1
                  end,
  
  Container = case Backend:get(ContainerName, FeedId) of
                {ok, Result} ->
                  Result;
                {error, _} when FeedId /= undefined ->
                  ContainerFields = proplists:get_value(ContainerName, ctail:containers()),
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
      ContainerTop = case Direction of
                       #iterator.prev -> #container.top;
                       #iterator.next -> #container.bottom
                     end,
      {Prev, Next} = case element(ContainerTop, Container) of
                       undefined ->
                         {undefined, undefined};
                       TailId ->
                         case Backend:get(Table, TailId) of
                           {error, _} ->
                             {undefined, undefined};
                           {ok, Tail} ->
                             InvDirection = case Direction of
                                              #iterator.next -> #iterator.prev;
                                              #iterator.prev -> #iterator.next
                                            end,
                             NewTail = setelement(InvDirection, Tail, Id),
                             Backend:put(NewTail),
                             TailId = element(#iterator.id, Tail),
                             case Direction of
                               #iterator.prev -> {TailId, undefined};
                               #iterator.next -> {undefined, TailId}
                             end
                         end
                     end,
      
      Container4 = case element(#container.count, Container) of
                     0 ->
                       ContainerX = setelement(#container.top, Container, Id),
                       setelement(#container.bottom, ContainerX, Id);
                     _ ->
                       Container
                   end,
      Container5 = setelement(ContainerTop, Container4, Id),
      Container6 = setelement(#container.count, Container5, element(#container.count, Container) + 1),

      Backend:put(Container6), %% Container

      Record1 = setelement(#iterator.next, Record, Next),
      Record2 = setelement(#iterator.prev, Record1, Prev),
      Record3 = setelement(#iterator.feed_id, Record2, element(#container.id, Container)),

      Backend:put(Record3), % Iterator

      {ok, Record3}
  end.

-spec relink(Container::tuple(), Record::tuple(), Backend::module()) -> ok | {error, _}.
relink(Container, Record, Backend) ->
  Table  = element(1, Record),
  Id     = element(#iterator.id, Record),
  Next   = element(#iterator.next, Record),
  Prev   = element(#iterator.prev, Record),
  Top    = element(#container.top, Container),
  Bottom = element(#container.bottom, Container),

  case Backend:get(Table, Prev) of
    {ok, Prev1} ->
      Prev2 = setelement(#iterator.next, Prev1, Next),
      Backend:put(Prev2);
    _ ->
      skip
  end,

  case Backend:get(Table, Next) of
    {ok, Next1} ->
      Next2 = setelement(#iterator.prev, Next1, Prev),
      Backend:put(Next2);
    _ ->
      skip
  end,

  Container1 = case Top of
                 Id -> setelement(#container.top, Container, Prev);
                 _  -> Container
               end,
  Container2 = case Bottom of
                 Id -> setelement(#container.bottom, Container1, Next);
                 _  -> Container1
               end,
  Container3 = setelement(#container.count, Container2, element(#container.count, Container2) - 1),

  %% TODO: check that feed works after purging

  Backend:put(Container3).

entries(Table, FeedId, StartId, StopId, Count, Direction, Backend) ->
  case Backend:get(feed, FeedId) of
    {ok, Container} ->
      FirstId = case StartId of
                  undefined ->
                    case Direction of
                      #iterator.prev -> element(#container.top, Container);
                      #iterator.next -> element(#container.bottom, Container)
                    end;
                  _ ->
                    StartId
                end,
      Count1 = case Count of
                 -1 ->
                   Count;
                 _ ->
                   case {StartId, StopId} of
                     {undefined, undefined} -> Count;
                     {undefined, _}         -> Count + 1;
                     {_, undefined}         -> Count + 1;
                     {_, _}                 -> Count + 2
                   end
                 end,
      Result = ctail:get(Table, FirstId, Backend),
      Records = iterate(Table, Result, StartId, StopId, Count1, Direction, Backend, []),
      case Direction of
        #iterator.next -> lists:reverse(Records);
        #iterator.prev -> Records
      end;
    {error, _} ->
      []
  end.

iterate(_Table, _Result, _StartId, _StopId, 0, _Direction, _Backend, []) -> [];
iterate(_Table, _Result, StartId, _StopId, 0, _Direction, _Backend, Acc) ->
  case StartId of
    undefined -> Acc;
    _         -> lists:droplast(Acc)
  end;
iterate(Table, Result, StartId, StopId, Count, Direction, Backend, Acc) ->
  case Result of
    {ok, Record} ->
      case element(#iterator.id, Record) of
        StopId ->
          iterate(Table, Result, StartId, StopId, 0, Direction, Backend, Acc);
        _ ->
          Acc1 = [Record|Acc],
          case element(Direction, Record) of
            undefined ->
              iterate(Table, Result, StartId, StopId, 0, Direction, Backend, Acc1);
            _ ->
              Result1 = ctail:get(Table, element(Direction, Record)),
              iterate(Table, Result1, StartId, StopId, Count-1, Direction, Backend, Acc1)
          end
      end;
    {error, _} ->
      iterate(Table, Result, StartId, StopId, 0, Direction, Backend, Acc)
  end.
