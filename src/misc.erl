%%%-------------------------------------------------------------------
%%% @author Emil Falk <emil.falk.1988@gmail.com>
%%% @copyright (C) 2012, Emil Falk
%%% @doc
%%% Utility functions and/or functions that doesn't fit anywhere else.
%%% @end
%%%-------------------------------------------------------------------
-module(misc).

%% API
-export([drop_return/2, drop_nth/2, random_elem/1, take_n_random/2,
         drop_random/1, drop_n_random/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec drop_return(N :: pos_integer(), List :: list(T)) -> list(T).
%% @pure
%% @doc Drops the N'th element of the List returning both the dropped element
%%      and the resulting list.
drop_return(N, List) ->
    drop_return(N, List, []).

-spec drop_nth(N :: pos_integer(), List :: list(T)) -> list(T).
%% @pure
%% @doc Drop the n'th element of a list
drop_nth(N, List0) ->
    {_, List} = drop_return(N, List0),
    List.

-spec random_elem(List :: list(T)) -> T.
%% @doc Get a random element of a list
random_elem(List) ->
    I = random:uniform(length(List)),
    lists:nth(I, List).

-spec take_n_random(N :: non_neg_integer(), List :: list(T)) -> list(T).
%% @doc Take N random elements from the list
take_n_random(N, List) -> 
    take_n_random(N, List, length(List)).

-spec drop_random(List :: list(T)) -> {T, list(T)}.
%% @doc Removes a random element from the list, returning
%%      a new list and the dropped element.
drop_random(List) ->
    N = random:uniform(length(List)),
    drop_return(N, List).

-spec drop_n_random(N :: pos_integer(), List :: list(T)) -> list(T).
%% @doc Removes n random elements from the list
drop_n_random(N, List) ->
    drop_n_random(List, N, length(List)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec take_n_random(N :: non_neg_integer(), List :: list(T),
                    Length :: non_neg_integer()) -> list(T).
%% @doc Helper function for take_n_random/2.
take_n_random(0, _List, _Length) ->
    [];
take_n_random(_N, _, 0) ->
    [];
take_n_random(N, List, Length) ->
    I = random:uniform(Length),
    {Elem, NewList} = drop_return(I, List),
    [Elem|take_n_random(N-1, NewList, Length-1)].

-spec drop_return(N :: pos_integer(), List :: list(T), Skipped :: list(T)) ->
                         {T, list(T)}.
%% @pure
%% @doc Helper function for drop_return/2
drop_return(1, [H|T], Skipped) ->
    {H, lists:reverse(Skipped) ++ T};
drop_return(N, [H|T], Skipped) ->
    drop_return(N-1, T, [H|Skipped]).

-spec drop_n_random(List :: list(T), N :: non_neg_integer(),
                    Length :: non_neg_integer()) -> list(T).                            
%% @doc Helper-function for drop_n_random/2
drop_n_random(List, 0, _Length) ->
    List;
drop_n_random(_List, _N, 0) ->
    [];
drop_n_random(List, N, Length) ->
    I = random:uniform(Length),
    drop_n_random(drop_nth(I, List), N-1, Length-1).
