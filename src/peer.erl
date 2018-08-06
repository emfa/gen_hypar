%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title A peer
%% @doc A peer is a supervisor with three children;
%%      A control process that talks with the hypar_node
%%      A send process that takes care of all sending
%%      A receive process that takes care of all receiveing
%%
%%      If either dies then the whole peer dies.
%%     
%%      This module also acts as an interface to:
%%      peer_outgoing, peer_ctl, peer_send, peer_recv
%% -------------------------------------------------------------------
-module(peer).
-behaviour(supervisor).
-include("gen_hypar.hrl").

%% supervisor callback
-export([init/1]).

%% Functions to create and manipulate active peers
-export([start_link/6, new/6, forward_join/3, shuffle/4, send_message/2,
         disconnect/1]).

-spec start_link(id(), id(), socket(), pid(), pid(), options()) -> {ok, pid()}.
%% @doc Start a peer (supervisor)
start_link(Identifier, Peer, Socket, GenHypar, HyparNode, Options) ->
    supervisor:start_link(?MODULE, [Identifier, Peer, Socket, GenHypar, HyparNode, Options]).

-spec new(pid(), id(), id(), socket(), pid(), options()) -> {ok, pid()}.
%% @doc Create a new peer, wait for the sub-process to spin up. The given
%%      socket is ready to become an active peer.
%%
%%      We need the property that link_up messages are received before any
%%      incoming messages. We let the receiver wait until it is safe.
%%
new(PeerSup, Myself, Peer, Socket, GenHypar, Options) ->
    Args = [Peer, Socket, GenHypar, self(), Options],
    {ok, _Pid} = supervisor:start_child(PeerSup, Args),
    {ok, CtlPid} = peer_ctl:wait_for(Myself, Peer),
    {ok, RecvPid} = peer_recv:wait_for(Myself, Peer),
    gen_tcp:controlling_process(Socket, RecvPid),
    gen_hypar:link_up(GenHypar, Peer, CtlPid),
    peer_recv:go_ahead(RecvPid, Socket),
    {ok, CtlPid}.

-spec forward_join(pid(), id(), ttl()) -> ok.
%% @doc Send a forward join to a peer
forward_join(Pid, Peer, TTL) ->
    peer_ctl:forward_join(Pid, Peer, TTL).

-spec shuffle(pid(), id(), ttl(), xlist()) -> ok.
%% @doc Send a shuffle to a peer
shuffle(Pid, Peer, TTL, XList) ->
    peer_ctl:shuffle(Pid, Peer, TTL, XList).

-spec send_message(pid(), iolist()) -> ok.
%% @doc Send a message to a peer
send_message(Pid, Msg) ->
    peer_ctl:send_message(Pid, Msg).

-spec disconnect(pid()) -> ok.
%% @doc Disconnect a peer, demonitor it
disconnect(Pid) ->
    peer_ctl:disconnect(Pid).

%% A peer consist of three processes, a control, a send and
%% a receive process.
init([Identifier, Peer, Socket, GenHypar, HyparNode, Options]) ->
    Timeout = gen_hypar_opts:timeout(Options),
    KeepAlive = gen_hypar_opts:keep_alive(Options),
    PeerCtl = {peer_ctl,
               {peer_ctl, start_link, [Identifier, Peer, Socket, GenHypar, HyparNode]},
               permanent, 5000, worker, [peer_ctl]},
    PeerSend = {peer_send,
                {peer_send, start_link, [Identifier, Peer, Socket, KeepAlive]},
                permanent, 5000, worker, [peer_send]},
    PeerRecv = {peer_recv,
                {peer_recv, start_link, [Identifier, Peer, Socket, Timeout]},
                permanent, 5000, worker, [peer_recv]},
    {ok, {{one_for_all, 0, 1}, [PeerCtl, PeerSend, PeerRecv]}}.
