%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Send peer process
%% @doc This is the send peer process. The control process feeds this
%%      process with outgoing messages(either data or control) to be
%%      sent over the socket. This also send periodic keep-alive messages,
%%      if the link is idle for too long.
%% @todo This is a good spot to implement rate throttling. Might be a good
%%       idea if there are lots of traffic. Implement keep-alive messages.
%%       This is way we can easier control the granularity of time units
%%       for tcp as a failure-detector.
%% -------------------------------------------------------------------
-module(peer_send).
-behaviour(gen_server).

-include("gen_hypar.hrl").

%% Start
-export([start_link/4]).

%% Coordination
-export([wait_for/2]).

%% Send functions
-export([send_message/2, forward_join/3, shuffle/4, disconnect/1]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2,
         code_change/3]).

%% State
-record(state, {local      :: id(),
                remote     :: id(),
                socket     :: socket(),
                keep_alive :: pos_integer()}).

-spec start_link(id(), id(), socket(), pos_integer()) -> {ok, pid()}.
%% @doc Start a send process
start_link(Identifier, Peer, Socket, KeepAlive) ->
    gen_server:start_link(?MODULE, [Identifier, Peer, Socket, KeepAlive], []).

-spec send_message(pid(), iolist()) -> ok.
%% @doc Send a data message
send_message(Pid, Msg) ->
    gen_server:cast(Pid, {message, Msg}).

-spec forward_join(pid(), id(), ttl()) -> ok.
%% @doc Send a forward join
forward_join(Pid, Peer, TTL) ->
    gen_server:cast(Pid, {forward_join, Peer, TTL}).

-spec shuffle(pid(), id(), ttl(), xlist()) -> ok.
%% @doc Send a shuffle
shuffle(Pid, Peer, TTL, XList) ->
    gen_server:cast(Pid, {shuffle, Peer, TTL, XList}).

-spec disconnect(pid()) -> ok.
%% @doc Send a disconnect
disconnect(Pid) ->
    gen_server:cast(Pid, disconnect).

init([Identifier, Peer, Socket, KeepAlive]) ->
    true = register_peer_send(Identifier, Peer),
    {ok, #state{local=Identifier,
                remote=Peer,
                socket=Socket,
                keep_alive=KeepAlive}}.

handle_cast({message, Bin}, S) ->
    proto_wire:send_message(S#state.socket, Bin),
    {noreply, S};

handle_cast({forward_join, Peer, TTL}, S) ->
    proto_wire:send_forward_join(S#state.socket, Peer, TTL),
    {noreply, S};

handle_cast({shuffle, Peer, TTL, XList}, S) ->
    proto_wire:send_shuffle(S#state.socket, Peer, TTL, XList),
    {noreply, S};
        
handle_cast(disconnect, S) ->
    proto_wire:send_disconnect(S#state.socket),
    {noreply, S}.

%% @doc Send a keep-alive message
handle_info(_, S) ->
    {stop, not_used, S}.

handle_call(_,_,S) ->
    {stop, not_used, S}.

terminate(_, S) ->
    gen_tcp:close(S#state.socket),
    ok.

code_change(_, S, _) ->
    {ok, S}.

-spec register_peer_send(id(), id()) -> true.
%% @doc Register a send process
register_peer_send(Identifier, Peer) ->
    gen_hypar_util:register(name(Identifier, Peer)).

-spec wait_for(id(), id()) -> {ok, pid()}.
%% @doc Wait for a send process to register
wait_for(Identifier, Peer) ->
    gen_hypar_util:wait_for(name(Identifier, Peer)).

-spec name(id(), id()) -> {peer_send, id(), id()}.
%% @doc The gproc name of a peer_send process
name(Identifier, Peer) ->
    {peer_send, Identifier, Peer}.
