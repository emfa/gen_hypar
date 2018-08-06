%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @title The gen_hypar behaviour
%% @doc This behaviour implements a peer process in an overlay defined by the
%%      hyparview protocol.
%%
%%      The behaviour needs to implement:
%%      -spec start_link(Identifier, ModuleArgs) -> {ok, Pid}.
%%
%%      This function should spawn of a process and immediatly call:
%%      gen_hypar:register_and_wait(Identifier)
%%
%%      This is needed to make sure that all processes are synchronized and
%%      that the process should be reachable from outside.
%% 
%%      The process can then use join_cluster to join a cluster, after that
%%      it will start to receive messages on the form:
%%      
%%         {message, From :: id(), Msg :: binary()}
%%         {link_up, {Peer :: id(), SendPid :: pid()}}
%%         {link_down, Peer :: id()}
%%
%%     The callback process can then use gen_hypar:send_message/2 to send data
%%     over an active link to it's neighbours in the overlay.
-module(gen_hypar).

-callback start_link(id(), any()) -> {ok, pid()}.

%% Operation
-export([start_link/4, join_cluster/2, get_peers/1, send_message/2]).

%% Registration & Coordination
-export([register_and_wait/1, wait_for/1, where/1]).

%% Incoming events
-export([deliver/3, link_up/3, link_down/2]).

-include("gen_hypar.hrl").

-spec start_link(id(), module(), any(), options()) -> {ok, pid()}.
%% @doc Start a new unconnected gen_hypar node.
%%      Identifier is the {Ip, Port} that the node is bound to.
%%      Module is the callback module implementing this behaviour
%%      ModuleArgs will be passed to the callback function on init
%%      Options are the gen_hypar options
%%
%%      Supervisor-tree:
%%                gen_hypar_sup
%%               /      |     \
%%              /       |      \
%%      hypar_node peer_sup gen_hypar
%%                 /||...\
%%                peer1..n
start_link(Identifier, Module, ModuleArgs, Options) ->
    gen_hypar_sup:start_link(Identifier, Module, ModuleArgs,
                             gen_hypar_opts:default(Options)).

-spec send_message(pid(), iolist()) -> ok.
%% @doc Send a message to a peer
send_message(Pid, Msg) ->
    peer:send_message(Pid, Msg).

-spec get_peers(pid()) -> list(peer()).
%% @doc Retrives peer ids and pids, should only be called from the gen_hypar process
get_peers(HyparNode) ->
    hypar_node:get_peers(HyparNode).

-spec join_cluster(pid(), id()) -> ok | {error, could_not_connect}.
%% @doc Join a node to a cluster.
join_cluster(HyparNode, Contact) ->
     hypar_node:join_cluster(HyparNode, Contact).

-spec register_and_wait(id()) -> {ok, pid()}.
%% @doc Register the gen_hypar process and wait until it's safe to continue
register_and_wait(Identifier) ->
    gen_hypar_util:register(name(Identifier)),
    hypar_node:wait_for(Identifier).

%% @doc Find the gen_hypar process with the given identifier
-spec where(id()) -> pid() | undefined.
where(Identifier) ->
    gproc:where({n, l, name(Identifier)}).

%% @private Wait for the gen_hypar process to start
-spec wait_for(id()) -> {ok, pid()}.
wait_for(Identifier) ->
    gen_hypar_util:wait_for(name(Identifier)).

%% @private Gproc of the gen_hypar process
-spec name(id()) -> {gen_hypar, id()}.
name(Identifier) ->
    {gen_hypar, Identifier}.

%% Incoming events

-spec deliver(pid(), id(), binary()) -> {message, id(), binary()}.
%% @private Used by the peer processes to deliver a message
deliver(GenHypar, Peer, Msg) ->
    GenHypar ! {message, Peer, Msg}.

-spec link_up(pid(), id(), pid()) -> {link_up, peer()}.
%% @private Used by the send process to signal a new link
link_up(GenHypar, Peer, Pid) ->
    GenHypar ! {link_up, {Peer, Pid}},
    ok.

-spec link_down(pid(), id()) -> {link_down, id()}.
%% @private When a peer either disconnects or fails
link_down(GenHypar, Peer) ->
    GenHypar ! {link_down, Peer}.
