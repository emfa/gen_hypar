%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Incoming peers and listener functions
%% @doc Start and stop the listener and handle incoming peers BEFORE
%%      they become active peers.
%% -------------------------------------------------------------------
-module(peer_incoming).
-behavior(ranch_protocol).

-include("gen_hypar.hrl").

-export([start_link/4, start_listener/2, stop_listener/1, incoming/3]).
-export([accept_neighbour_request/1, decline_neighbour_request/1, close/1]).

%% @doc Ranch callback function, starts an new so far temporary process.
%%      It will try to receive as much data as possible to then transfer
%%      control to the hypar_node.
-spec start_link(pid(), socket(), module(), any()) -> {ok, pid()}.
start_link(ListenerPid, Socket, _Transport, Args) ->
    {ok, spawn_link(?MODULE, incoming, [ListenerPid, Socket, Args])}.

-spec start_listener(id(), options()) -> {ok, pid()}.
%% @doc Start up a ranch listener, closing the old one if it exists.
start_listener({Ip, Port}=Identifier, Options) ->
    stop_listener(Identifier),
    ranch:start_listener({gen_hypar, Identifier}, 20, ranch_tcp,
                         [{ip, Ip}, {port, Port}], ?MODULE, {self(), Options}).

-spec stop_listener(id()) -> ok | {error, not_found}.
%% @doc Stop the ranch listener.
stop_listener(Identifier) ->    
    ranch:stop_listener({gen_hypar, Identifier}).

-spec accept_neighbour_request(socket()) -> ok.
%% @doc Accept a pending peer
accept_neighbour_request(Socket) ->
    proto_wire:send_accept(Socket).

-spec decline_neighbour_request(socket()) -> ok.
%% @doc Decline a pending peer
decline_neighbour_request(Socket) ->
    proto_wire:send_decline(Socket).

-spec close(socket()) -> ok.
%% @doc Close a socket
close(Socket) ->
    proto_wire:close(Socket).

-spec incoming(pid(), socket(), {pid(), options()}) -> ok.
%% @doc Start to receive an incoming connection and then transfer control
%%      to the hypar node.
incoming(ListenerPid, Socket, {HyparNode, Options}) ->
    ok = ranch:accept_ack(ListenerPid),
    case proto_wire:handle_incoming_connection(Socket, Options) of
        {join, Peer} ->
            gen_tcp:controlling_process(Socket, HyparNode),    
            hypar_node:join(HyparNode, Peer, Socket);
        {join_reply, Peer} ->
            gen_tcp:controlling_process(Socket, HyparNode),    
            hypar_node:join_reply(HyparNode, Peer, Socket);
        {neighbour, Peer, Priority} ->
            gen_tcp:controlling_process(Socket, HyparNode),
            hypar_node:neighbour(HyparNode, Peer, Priority, Socket);
        {shuffle_reply, XList} ->
            hypar_node:shuffle_reply(HyparNode, XList),
            close(Socket)
    end.
