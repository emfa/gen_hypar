%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Top level supervisor
%% @doc Top level supervisor for gen_hypar
%% -------------------------------------------------------------------
-module(gen_hypar_sup).
-behaviour(supervisor).

-include("gen_hypar.hrl").

%% Start
-export([start_link/4]).

%% supervisor callback
-export([init/1]).

-spec start_link(id(), module(), any(), options()) -> {ok, pid()}.
%% @doc Start the top level supervisor
start_link(Identifier, Mod, ModArgs, Options) ->
    supervisor:start_link(?MODULE, [Identifier, Mod, ModArgs, Options]).

init([Identifier, Mod, ModArgs, Options]) ->
    HyparNode = {hypar_node,
                 {hypar_node, start_link, [Identifier, Options]},
                 permanent, 5000, worker, [hypar_node]},
    PeerSup = {peer_sup,
               {peer_sup, start_link, [Identifier]},
               permanent, 5000, supervisor, [peer_sup]},
    GenHypar = {gen_hypar,
                {Mod, start_link, [Identifier, ModArgs]},
                permanent, 5000, worker, [gen_hypar, Mod]},
    {ok, { {one_for_all, 5, 10}, [HyparNode, PeerSup, GenHypar]}}.
