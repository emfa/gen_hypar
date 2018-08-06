%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Control peer process
%% @doc This is the main peer process. The hypar_node and gen_hypar process
%%      communicate with this process with in turn sends and receives messages
%%      via peer_send and peer_recv.
%% -------------------------------------------------------------------
-module(peer_ctl).
-behaviour(gen_server).

-include("gen_hypar.hrl").

%% Start a control peer
-export([start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Outgoing events
-export([send_message/2, forward_join/3, shuffle/4,
         disconnect/1]).

%% Incoming events
-export([incoming_message/2]).

%% Coordination
-export([wait_for/2]).

%% State
-record(state, {local       :: id(),
                remote      :: id(),
                socket      :: inet:socket(),
                gen_hypar   :: pid(),
                hypar_node  :: pid(),
                peer_send   :: pid()}).

-spec start_link(id(), id(), socket(), pid(), pid()) -> {ok, pid()}.
%% @doc Start a control peer
start_link(Identifier, Peer, Socket, GenHypar, HyparNode) ->
    Args = [Identifier, Peer, Socket, GenHypar, HyparNode],
    gen_server:start_link(?MODULE, Args, []).

-spec send_message(pid(), iolist()) -> ok.
%% @doc Send a message to a peer
send_message(Pid, Msg) ->
    gen_server:cast(Pid, {message, Msg}).

-spec incoming_message(pid(), active_message()) -> ok.
%% @doc Receive an incoming message from a peer
incoming_message(Pid, Msg) ->
    gen_server:cast(Pid, {incoming, Msg}).

-spec forward_join(pid(), id(), ttl()) -> ok.
%% @doc Send a forward join to a peer
forward_join(Pid, Peer, TTL) ->
    gen_server:cast(Pid, {forward_join, Peer, TTL}).

-spec shuffle(pid(), id(), ttl(), xlist()) -> ok.
%% @doc Send a shuffle to a peer
shuffle(Pid, Peer, TTL, XList) ->
    gen_server:cast(Pid, {shuffle, Peer, TTL, XList}).

-spec disconnect(pid()) -> ok.
%% @doc Send a disconnect message to a peer
disconnect(Pid) ->
    gen_server:cast(Pid, disconnect).

init([Identifier, Peer, Socket, GenHypar, HyparNode]) ->    
    {ok, #state{local=Identifier,
                remote=Peer,
                socket=Socket,
                gen_hypar=GenHypar,
                hypar_node=HyparNode}, 0}.

handle_cast(_, disconnected) ->
    {noreply, disconnected};
%% Handle an incoming message
handle_cast({incoming, Msg}, S) ->
    handle_incoming(Msg, S);
%% Route a message over to the remote peer
handle_cast({message, Msg}, S) ->
    peer_send:send_message(S#state.peer_send, Msg),
    {noreply, S};
%% Send a forward join to the remote peer
handle_cast({forward_join, Peer, TTL}, S) ->
    peer_send:forward_join(S#state.peer_send, Peer, TTL),
    {noreply, S};
%% Send a shuffle request to the remote peer
handle_cast({shuffle, Peer, TTL, XList}, S) ->
    peer_send:shuffle(S#state.peer_send, Peer, TTL, XList),
    {noreply, S};
%% Disconnect the remote peer, move to disconnected state and 
%% wait for the other side to kill the socket
%% NOTE: This is because we DO NOT want messages to be received after
%%       the link_down event has been sent. This would violate the
%%       properties that when a link_down event has been sent the link
%%       is really DOWN. If we didn't guard here the receive process
%%       might continue to deliver messages and the gen_hypar process
%%       might 'maliciously' send data after the link_down. 
handle_cast(disconnect, S) ->
    peer_send:disconnect(S#state.peer_send),
    {noreply, disconnected}.

handle_call(_, _, State) ->
    {stop, not_used, State}.

%% Wait in the send process, then register the control peer
handle_info(timeout, S) ->
    {ok, SendPid} = peer_send:wait_for(S#state.local, S#state.remote),
    true = register_peer_ctl(S#state.local, S#state.remote),
    {noreply, S#state{peer_send=SendPid}}.

terminate(_, S) ->
    gen_tcp:close(S#state.socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec handle_incoming(active_message(), #state{}) ->
                             {noreply, #state{}} |
                             {stop, normal, #state{}}.
%% @doc Handle an incoming message, send it to the gen_hypar process
handle_incoming({message, Bin}, S) ->
    gen_hypar:deliver(S#state.gen_hypar, S#state.remote, Bin),
    {noreply, S};
%% Send an incoming forward join to the hypar_node
handle_incoming({forward_join, Peer, TTL}, S) ->
    hypar_node:forward_join(S#state.hypar_node, S#state.remote, Peer, TTL),
    {noreply, S};
%% Send an incoming shuffle to the hypar_node
handle_incoming({shuffle, Peer, TTL, XList}, S) ->
    hypar_node:shuffle(S#state.hypar_node, S#state.remote, Peer, TTL, XList),
    {noreply, S};
%% Just ignore a keep-alive
handle_incoming(keep_alive, S) ->
    {noreply, S};
%% Send an incoming disconnect to the hypar_node and shut down the peer
handle_incoming(disconnect, S) ->
    hypar_node:disconnect(S#state.hypar_node, S#state.remote),
    {stop, normal, S}.

-spec register_peer_ctl(id(), id()) -> true.
%% @doc Register a control peer
register_peer_ctl(Identifier, Peer) ->
    gen_hypar_util:register(name(Identifier, Peer)).

-spec wait_for(id(), id()) -> {ok, pid()}.
%% @doc Wait for a control peer to start
wait_for(Identifier, Peer) ->
    gen_hypar_util:wait_for(name(Identifier, Peer)).

-spec name(id(), id()) -> {peer_ctl, id(), id()}.
%% @doc Gproc name of a control peer
name(Identifier, Peer) ->
    {peer_ctl, Identifier, Peer}.
