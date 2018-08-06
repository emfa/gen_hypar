%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Outgoing connections
%% @doc Handle outgoing peer connections. This is either new pending peers or
%%      temporary shuffle peers. This module is used BEFORE outgoing peers
%%      become active.
%% -------------------------------------------------------------------
-module(peer_outgoing).

-include("gen_hypar.hrl").

-export([join/3, join_reply/3, neighbour/4, shuffle_reply/4]).

-spec join(id(), id(), options()) -> {ok, socket()}.
%% @doc Start a new connection and fire away a join
join(Myself, Peer, Options) ->
    {ok, Socket} = proto_wire:start_connection(Myself, Peer, Options),
    proto_wire:send_join(Socket, Myself),
    {ok, Socket}.

-spec join_reply(id(), id(), options()) -> {ok, socket()}.
%% @doc Start a new outgoing connection and fire away a join-reply
join_reply(Myself, Peer, Options) ->
    {ok, Socket} = proto_wire:start_connection(Myself, Peer, Options),
    proto_wire:send_join_reply(Socket, Myself),
    {ok, Socket}.

-spec neighbour(id(), id(), priority(),options()) -> {ok, socket()}.
%% @doc Start a new outgoing connection and try to become neighbours
neighbour(Myself, Peer, Priority, Options) ->
    Timeout = gen_hypar_opts:timeout(Options),
    {ok, Socket} = proto_wire:start_connection(Myself, Peer, Options),
    proto_wire:send_neighbour_request(Socket, Myself, Priority),
    case proto_wire:wait_for_neighbour_reply(Socket, Timeout) of
        accept  -> {ok, Socket};
        decline -> decline
    end.

-spec shuffle_reply(id(), id(), id(), options()) -> pid().
%% @doc Since shuffle replys are just temporary connections we spawn a temp-
%%      process.
shuffle_reply(Myself, Peer, XList, Options) ->
    spawn(fun() -> do_shuffle_reply(Myself, Peer, XList, Options) end).

-spec do_shuffle_reply(id(), id(), id(), options()) -> ok.          
%% @doc Start a socket and just fire away the shuffle reply
do_shuffle_reply(Myself, Peer, XList, Options) ->    
    {ok, Socket} = proto_wire:start_connection(Myself, Peer, Options),
    proto_wire:send_shuffle_reply(Socket, XList).
