%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title The peer supervisor
%% @doc The peer supervisor
%% -------------------------------------------------------------------
-module(peer_sup).
-behaviour(supervisor).

-include("gen_hypar.hrl").

%% Start
-export([start_link/1]).

%% Coordination
-export([wait_for/1]).

%% supervisor callback
-export([init/1]).

-spec start_link(id()) -> {ok, pid()}.
%% @doc Start the peer supervisor
start_link(Identifier) ->
    supervisor:start_link(?MODULE, [Identifier]).

-spec wait_for(id()) -> {ok, pid()}.
%% @doc Wait for the peer supervisor to start
wait_for(Identifier) ->
    gen_hypar_util:wait_for(name(Identifier)).

init([Identifier]) ->
    register_peer_sup(Identifier),
    Peer = {peer,
            {peer, start_link, [Identifier]},
            temporary, 5000, supervisor, [peer]},
    {ok, {{simple_one_for_one, 5, 10}, [Peer]}}.

-spec register_peer_sup(id()) -> true.
%% @doc Register the peer supervisor
register_peer_sup(Identifier) ->
    gen_hypar_util:register(name(Identifier)).

-spec name(id()) -> {peer_sup, id()}.
%% @doc Gproc name of the peer supervisor
name(Identifier) ->
    {peer_sup, Identifier}.
