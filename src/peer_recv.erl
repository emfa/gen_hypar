%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Receive peer process
%% @doc This is the receiving peer process. It receives data on the
%%      socket and parses the packets as they come and forwards them
%%      to the control process.
%% @todo Analog to peer_send this might be a good place to do buffering
%%       of outgoing messages for rate throttling. TCP should do most of
%%       the rate controlling but it may not be enough to get a good solution.
%% -------------------------------------------------------------------
-module(peer_recv).
-behaviour(gen_server).

-include("gen_hypar.hrl").

%% Start
-export([start_link/4]).

%% Coordination
-export([go_ahead/2, wait_for/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2,
         code_change/3]).

%% State
-record(state, {peer_ctl :: pid(),
                socket   :: socket(),
                timeout  :: timeout(),
                data     :: binary()}).

-spec start_link(id(), id(), socket(), timeout()) -> {ok, pid()}.
%% @doc Start a reciving process
start_link(Identifier, Peer, Socket, Timeout) ->
    gen_server:start_link(?MODULE, [Identifier, Peer, Socket, Timeout], []).

-spec go_ahead(pid(), socket()) -> {go_ahead, socket()}.
%% @doc Tell the receiving process that it's ok to proceed.
go_ahead(Pid, Socket) ->
    Pid ! {go_ahead, Socket}.

init([Identifier, Peer, Socket, Timeout]) ->
    {ok, Pid} = peer_ctl:wait_for(Identifier, Peer),
    register_peer_send(Identifier, Peer),
    {ok, #state{peer_ctl=Pid,
                socket=Socket,
                timeout=Timeout,
                data = <<>> }}.

handle_cast(_, S) ->
    {stop, not_used, S}.

handle_call(_, _, S) ->
    {stop, not_used, S}.

handle_info(timeout, S=#state{socket=Socket, data=RestBin}) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            handle_packet(S, <<RestBin/binary, Bin/binary>>);
        _Err ->
            {stop, normal, S}
    end;

handle_info({go_ahead, Socket}, S=#state{socket=Socket}) ->
    {noreply, S, 0}.

terminate(_, S) ->
    gen_tcp:close(S#state.socket),
    ok.

code_change(_, S, _) ->
    {ok, S}.

-spec handle_packet(#state{}, binary()) -> {noreply, #state{}}.
%% @doc Handle an incoming packet, decoding it and send it to the control
%%      process
handle_packet(S, <<>>) ->
    {noreply, S, 0};
handle_packet(S, <<Len:32, Bin/binary>>) when byte_size(Bin) >= Len ->
    <<Packet:Len/binary, Rest/binary>> = Bin,
    Msg = proto_wire:decode_msg(Packet),
    peer_ctl:incoming_message(S#state.peer_ctl, Msg),
    handle_packet(S, Rest);
handle_packet(S, Bin) ->
    {noreply, S#state{data=Bin}, 0}.

-spec register_peer_send(id(), id()) -> true.
%% @doc Register the receiving process
register_peer_send(Identifier, Peer) ->
    gen_hypar_util:register(name(Identifier, Peer)).

-spec wait_for(id(), id()) -> {ok, pid()}.
%% @doc Wait for a receiving process
wait_for(Identifier, Peer) ->
    gen_hypar_util:wait_for(name(Identifier, Peer)).

-spec name(id(), id()) -> {peer_recv, id(), id()}.
%% @doc Name of a receive process
name(Identifier, Peer) ->
    {peer_recv, Identifier, Peer}.
