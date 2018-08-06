%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @title Options related
%% @doc Code related to various options in gen_hypar. Mostly to nicen things up.
%% -------------------------------------------------------------------
-module(gen_hypar_opts).

-include("gen_hypar.hrl").

-export([arwl/1, prwl/1, active_size/1, passive_size/1, k_active/1,
         k_passive/1, shuffle_period/1, timeout/1, send_timeout/1, keep_alive/1,
         default/1]).

%% @doc Esthetic functions
-spec arwl(options())           -> pos_integer().
-spec prwl(options())           -> pos_integer().
-spec active_size(options())    -> pos_integer().
-spec passive_size(options())   -> pos_integer().
-spec k_active(options())       -> pos_integer().
-spec k_passive(options())      -> pos_integer().
-spec shuffle_period(options()) -> pos_integer().
-spec timeout(options())        -> pos_integer().
-spec send_timeout(options())   -> pos_integer().
-spec keep_alive(options())     -> pos_integer().
arwl(Opts)           -> proplists:get_value(arwl, Opts).
prwl(Opts)           -> proplists:get_value(prwl, Opts).
active_size(Opts)    -> proplists:get_value(active_size, Opts).
passive_size(Opts)   -> proplists:get_value(passive_size, Opts).      
k_active(Opts)       -> proplists:get_value(k_active, Opts).
k_passive(Opts)      -> proplists:get_value(k_passive, Opts).
shuffle_period(Opts) -> proplists:get_value(shuffle_period, Opts).
timeout(Opts)        -> proplists:get_value(timeout, Opts).
send_timeout(Opts)   -> proplists:get_value(send_timeout, Opts).
keep_alive(Opts)     -> proplists:get_value(keep_alive, Opts).

-spec default(options()) -> options().
%% @doc Check so all neccessary options are defined, otherwise default them.
default(Options) ->
    lists:foldl(fun({Opt, _}=OptPair, Acc0) ->
                        case proplists:is_defined(Opt, Acc0)  of
                            true -> Acc0;
                            false -> [OptPair|Acc0]
                        end
                end, Options, default_options()).

-spec default_options() -> options().
%% @doc Default options for the hyparview-application
default_options() ->
    [{active_size, 5},
     {passive_size, 30},
     {arwl, 6},
     {prwl, 3},
     {k_active, 3},
     {k_passive, 4},
     {shuffle_period, 10000},
     {timeout, 10000},
     {send_timeout, 10000},
     {keepalive, 10000}].
