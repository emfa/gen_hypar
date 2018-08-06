%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title HyParView node logic
%% @doc This module implements the node logic in the HyParView-protocol
%% -------------------------------------------------------------------
-module(hypar_node).
-behaviour(gen_server).

-include("gen_hypar.hrl").

%%%===================================================================
%%% Exports
%%%===================================================================

%% Operations
-export([start_link/2, join_cluster/2, get_peers/1]).

%% Incoming events
-export([join/3, join_reply/3, forward_join/4, neighbour/4, disconnect/2,
         shuffle/5, shuffle_reply/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

%% Registration
-export([wait_for/1]).

%%%===================================================================
%%% hypar_node state
%%%===================================================================

-record(st, {id              :: id(),           %% This nodes identifier
             gen_hypar       :: pid(),          %% Pid of the gen_hypar process
             peer_sup        :: pid(),          %% Pid of the peer supervisor
             activev = []    :: active_view(),  %% The active view
             passivev = []   :: passive_view(), %% The passive view
             last_xlist = [] :: xlist(),        %% The last shuffle xlist sent
             opts            :: options()       %% Options
            }).

%%%===================================================================
%%% Operation
%%%===================================================================

-spec start_link(id(), options()) -> {ok, pid()}.
%% @doc Start the hypar_node with given id and with <em>Options</em>.
start_link(Identifier, Options) ->
    gen_server:start_link(?MODULE, [Identifier, Options], []).

-spec join_cluster(pid(), id()) -> ok | {error, could_not_connect}.
%% @doc Try to join node to a cluster.
join_cluster(HyparNode, Contact) ->
    gen_server:call(HyparNode, {join_cluster, Contact}).

-spec get_peers(pid()) -> list(peer()).
%% @doc Get all peers.
get_peers(HyparNode) ->
    gen_server:call(HyparNode, get_peers).

%%%===================================================================
%%% Incoming events
%%%===================================================================

-spec join(pid(), id(), socket()) -> ok.
%% @doc Join a peer to a node.
join(HyparNode, Peer, Socket) ->
    gen_server:cast(HyparNode, {join, Peer, Socket}).

-spec forward_join(pid(), id(), id(), ttl()) -> ok.
%% @doc A forward join event carrying a new node and a ttl to the node
forward_join(HyparNode, Peer, NewNode, TTL) ->
    gen_server:cast(HyparNode, {forward_join, Peer, NewNode, TTL}).

-spec join_reply(pid(), id(), socket()) -> ok.
%% @doc A join reply from a peer to the node
join_reply(HyparNode, Peer, Socket) ->
    gen_server:cast(HyparNode, {join_reply, Peer, Socket}).

-spec neighbour(pid(), id(), priority(), inet:socket()) -> ok.
%% @doc Neighbour request from a peer to the node
neighbour(HyparNode, Peer, Priority, Socket) ->
    gen_server:cast(HyparNode, {neighbour, Peer, Priority, Socket}).

-spec disconnect(pid(), id()) -> ok.
%% @doc Disconnect a peer from the node
disconnect(HyparNode, Peer) ->
    gen_server:cast(HyparNode, {disconnect, Peer}).

-spec shuffle(pid(), pid(), id(), ttl(), xlist()) -> ok.
%% @doc A shuffle request to the node from a peer
shuffle(HyparNode, Peer, Requester, TTL, XList) ->
    gen_server:cast(HyparNode, {shuffle, Peer, Requester, TTL, XList}).

-spec shuffle_reply(pid(), xlist()) -> ok.
%% @doc A shuffle reply to the node from a temporary peer
shuffle_reply(HyparNode, ReplyXList) ->
    gen_server:cast(HyparNode, {shuffle_reply, ReplyXList}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% Initialize
init([Identifier, Options]) ->
    random:seed(now()),    
    {ok ,#st{id=Identifier, opts=Options}, 0}.

%% Retrive the peers
handle_call(get_peers, _, S) ->
    {reply, remove_mrefs(S#st.activev), S};

%% Join a cluster via given contact nodes
handle_call({join_cluster, Peer}, _, S) ->
    #st{id=Myself, opts=Options} = S,
    try peer_outgoing:join(Myself, Peer, Options) of
        {ok, Socket} ->
            {reply, ok, add_active_peer(Peer, Socket, S)}
    catch
        error:_ ->
            {reply, {error, could_not_connect}, S}
    end.

%% Add newly joined node to active view, propagate forward joins. The socket
%% has finished the handshake and is ready to become an active peer
handle_cast({join, Peer, Socket}, S0) ->
    #st{id=Myself, activev=ActiveV, opts=Options} = S0,
    case peer_ok(Peer, Myself, ActiveV) of
        true ->
            S = add_active_peer(Peer, Socket, S0),
            ARWL = gen_hypar_opts:arwl(Options),
            ok = send_forward_joins(Peer, ARWL, S#st.activev),
            {noreply, S};
        false ->
            peer_incoming:close(Socket),
            {noreply, S0}
    end;

%% Accept a connection from the join procedure. The socket has gone through the
%% whole handshake and is ready to become active
handle_cast({join_reply, Peer, Socket}, S) ->
    #st{id=Myself, activev=ActiveV} = S,
    case peer_ok(Peer, Myself, ActiveV) of
        true ->
            {noreply, add_active_peer(Peer, Socket, S)};
        false ->
            peer_incoming:close(Socket),
            {noreply, S}
    end;

%% Neighbour request, either accept or decline based on priority and current
%% active view. This connection/socket has not finished the whole handshake
%% and thus we need to either accept or decline it before making it an active
%% peer
handle_cast({neighbour, Peer, Priority, Socket}, S) ->
    #st{activev=ActiveV, opts=Options} = S,
    case Priority of
        %% High priority neighbour request thus the node needs to accept
        %% the request what ever the current active view is
        high ->
            accept_neighbour(Peer, Socket, S);
        %% Low priority request, only accept if we have space in the
        %% nodes active view
        low ->            
            case length(ActiveV) < gen_hypar_opts:active_size(Options) of
                true  ->
                    accept_neighbour(Peer, Socket, S);
                false ->
                    decline_neighbour(Socket, S)
            end
    end;

%% Disconnect an open active connection, add disconnecting node to passive view
handle_cast({disconnect, Peer}, S) ->
    #st{activev=ActiveV, passivev=PassiveV, gen_hypar=GenHypar} = S,    
    %% Demonitor & link_down
    {Peer, _, MRef} = lists:keyfind(Peer, 1, ActiveV),
    erlang:demonitor(MRef, [flush]),
    gen_hypar:link_down(GenHypar, Peer),
    {noreply, S#st{activev=remove_active(Peer, ActiveV),
                   passivev=add_passive(Peer, PassiveV)}};

%% Respond to a forward_join, add to active or propagate and maybe add to
%% passive view.
handle_cast({forward_join, Peer, NewPeer, TTL}, S0) ->
    #st{activev=ActiveV, opts=Options} = S0,
    case TTL =:= 0 orelse length(ActiveV) =:= 1 of
        true ->
            %% Add to active view, send a join_reply to let the
            %% other node know, check to see that you don't send to yourself
            accept_forward_join(NewPeer, S0);                
        false ->
            %% Add to passive view if TTL is equal to PRWL
            S1 = case TTL =:= gen_hypar_opts:prwl(Options) of
                     true  -> add_passive_peer(NewPeer, S0);
                     false -> S0
                 end,
            
            %% Propagate the forward join using a random walk
            AllBut = remove_active(Peer, ActiveV),
            {_, Pid, _} = gen_hypar_util:random_elem(AllBut),
            ok = peer:forward_join(Pid, NewPeer, TTL-1),

            {noreply, S1}
    end;

%% Respond to a shuffle request, either propagate it or accept it via a
%% temporary connection to the source of the request. If the node accept then
%% it adds the shuffle list into it's passive view and responds with with
%% a shuffle reply
handle_cast({shuffle, Peer, Req, TTL, XList}, S) ->
    #st{id=Myself, activev=ActiveV, passivev=PassiveV, opts=Options} = S,
    case TTL > 0 andalso length(ActiveV) > 1 of
        %% Propagate the random walk
        true ->
            AllBut = remove_active(Peer, ActiveV),
            {_ ,Pid, _} = gen_hypar_util:random_elem(AllBut),
            ok = peer:shuffle(Pid, Req, TTL-1, XList),
            {noreply, S};
        %% Accept the shuffle request, add to passive view and reply
        false ->
            RXList = gen_hypar_util:take_n_random(length(XList), PassiveV),
            peer_outgoing:shuffle_reply(Myself, Req, RXList, Options),
            {noreply, add_xlist(S, XList, RXList)}
    end;

%% Accept a shuffle reply, add the reply list into the passive view and
%% close the temporary connection.
handle_cast({shuffle_reply, ReplyXList}, S0) ->
    S = add_xlist(S0#st{last_xlist=[]}, ReplyXList, S0#st.last_xlist),
    {noreply, S}.

%% Handle failing connections. Try to find a new one if possible
handle_info({'DOWN', MRef, process, Pid, _}, S0) ->
    #st{activev=ActiveV, gen_hypar=GenHypar} = S0,
    {Peer, Pid, MRef} = lists:keyfind(MRef, 3, ActiveV),
    gen_hypar:link_down(GenHypar, Peer),
    S = S0#st{activev=remove_active(Pid, ActiveV)},
    {noreply, find_new_active(S)};

%% Timer message for periodic shuffle. Send of a shuffle request to a random
%% peer in the active view. Ignore if we don't have any active connections.
handle_info(shuffle, S) ->
    #st{id=Myself, activev=ActiveV, opts=Options} = S,
    shuffle_timer(gen_hypar_opts:shuffle_period(Options)),

    case ActiveV =:= [] of
        %% No active peers to send shuffle to
        true ->
            {noreply, S#st{last_xlist=[]}};
        
        %% Send the shuffle to a random peer
        false ->
            {_,Pid, _} = gen_hypar_util:random_elem(ActiveV),
            TTL = gen_hypar_opts:arwl(Options)-1,
            XList = create_xlist(S),
            ok = peer:shuffle(Pid, Myself, TTL, XList),
            {noreply, S#st{last_xlist=XList}}
    end;

%% Initialize
handle_info(timeout, S) ->
    #st{id=Myself, opts=Options} = S,
    %% Wait for gen_hypar process to start
    {ok, GenHypar} = gen_hypar:wait_for(Myself),
    %% Wait for the peer supervisor to start
    {ok, PeerSup} = peer_sup:wait_for(Myself),

    %% Spin up the listener
    peer_incoming:start_listener(Myself, Options),

    %% Register ourselves
    register_hypar_node(Myself),

    %% Start shuffle
    shuffle_timer(gen_hypar_opts:shuffle_period(Options)),
    {noreply, S#st{gen_hypar=GenHypar, peer_sup=PeerSup}}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_, S) ->
    peer_incoming:stop_listener(S#st.id),
    ok.

-spec accept_forward_join(id(), #st{}) -> {noreply, #st{}}.
%% @doc Accept a forward join, just discard if there are problems
accept_forward_join(Peer, S0) ->
    #st{id=Myself, activev=ActiveV, opts=Options} = S0,
    case peer_ok(Peer, Myself, ActiveV) of
        true ->
            try peer_outgoing:join_reply(Myself, Peer, Options) of
                {ok, Socket} ->
                    {noreply, add_active_peer(Peer, Socket, S0)}
            catch
                error:_ ->
                    {noreply, S0}
            end;
        false ->
            {noreply, S0}
    end.

-spec send_forward_joins(id(), ttl(), active_view()) -> ok.
%% @doc send out forward joins after an incoming join
send_forward_joins(NewPeer, TTL, ActiveV) ->
    ForwardFun = fun(Pid) -> peer:forward_join(Pid, NewPeer, TTL) end,
    lists:foreach(ForwardFun, get_pids(remove_active(NewPeer, ActiveV))).

-spec accept_neighbour(id(), socket(), #st{}) -> {noreply, #st{}}.
%% @doc Accept a neighbour request and spin up a peer, discard if any
%%          errors occur.
accept_neighbour(Peer, Socket, S) ->
    try peer_incoming:accept_neighbour_request(Socket) of
        ok ->
            {noreply, add_active_peer(Peer, Socket, S)}
    catch
        error:_ ->
            {noreply, S}
    end.

-spec decline_neighbour(socket(), #st{}) -> {noreply, #st{}}.
%% @doc Decline a neighbour request
decline_neighbour(Socket, S) ->
    peer_incoming:decline_neighbour_request(Socket),
    {noreply, S}.

%%%===================================================================
%%% Active view related
%%%===================================================================

-spec add_active_peer(id(), socket(), #st{}) -> #st{}.
%% @doc Add a peer to the active view, removing a peer if necessary. The new
%%      state is returned. If a node has to be dropped, then it is informed
%%      via a DISCONNECT message and placed in the passive view.
add_active_peer(Peer, Socket, S0) ->
    #st{id=Myself, activev=ActiveV0, gen_hypar=GenHypar, peer_sup=PeerSup,
        opts=Options} = S0,
    S = case length(ActiveV0) >= gen_hypar_opts:active_size(Options) of
            true  -> drop_random_active(S0);
            false -> S0
        end,
    
    {ok, Pid} = peer:new(PeerSup, Myself, Peer, Socket, GenHypar, Options),
    MRef = erlang:monitor(process, Pid),
    
    S#st{activev=add_active({Peer, Pid, MRef}, S#st.activev),
         passivev=remove_passive(Peer, S#st.passivev)}.

-spec drop_random_active(#st{}) -> #st{}.
%% @doc Drop a random node from the active view  down to the passive
%%      view. Send a disconnect message to the dropped node.
drop_random_active(S) ->
    #st{activev=ActiveV0, passivev=PassiveV0, gen_hypar=GenHypar,
        opts=Options} = S,

    Slots = length(PassiveV0)-gen_hypar_opts:passive_size(Options)+1,
    {{Peer, Pid, MRef}, ActiveV} = gen_hypar_util:drop_random_element(ActiveV0),
    PassiveV = add_passive(Peer, gen_hypar_util:drop_n_random(Slots, PassiveV0)),

    gen_hypar:link_down(GenHypar, Peer),
    ok = peer:disconnect(Pid),
    erlang:demonitor(MRef, [flush]),
    
    S#st{activev=ActiveV, passivev=PassiveV}.

-spec find_new_active(#st{}) -> #st{}.
%% @doc Find a new active peer. The function will send neighbour requests to
%%      nodes in passive view until it finds a good one or no more good exist.
find_new_active(S) ->
    #st{id=Myself, activev=ActiveV, passivev=PassiveV0, opts=Options} = S,
    Priority = get_priority(ActiveV),

    case find_neighbour(Myself, Priority, PassiveV0, Options) of
        {no_valid, PassiveV} ->
            S#st{passivev=PassiveV};
        {{Peer, Socket}, PassiveV} ->
            add_active_peer(Peer, Socket, S#st{passivev=PassiveV})
    end.

-spec find_neighbour(id(), priority(), passive_view(), options()) ->
                            {{id(), socket()} | no_valid, passive_view()}.
%% @doc Try to find a new active neighbour
find_neighbour(Myself, Priority, PassiveV, Options) ->
    find_neighbour(Myself, Priority, PassiveV, Options, []).

-spec find_neighbour(id(), priority(), passive_view(), options(), view()) ->
                            {{id(), socket()} | no_valid, passive_view()}.
%% @doc Helper function for find_neighbour/3.
find_neighbour(_, _, [], _, Tried) ->
    {no_valid, Tried};
find_neighbour(Myself, Priority, PassiveV0, Options, Tried) ->
    {Peer, Passive} = gen_hypar_util:drop_random_element(PassiveV0),
    try peer_outgoing:neighbour(Myself, Peer, Priority, Options) of
        {ok, Socket} ->
            {{Peer, Socket}, Passive ++ Tried};
        decline ->
            find_neighbour(Myself, Priority, Passive, Options,
                           [Peer|Tried])
    catch
        error:_ ->
            find_neighbour(Myself, Priority, Passive, Options, Tried)
    end.

-spec get_ids(active_view()) -> view().
%% @doc Pick out all ids of the peers
get_ids(ActiveV) ->
    [Peer || {Peer,_,_} <- ActiveV].

-spec get_pids(active_view()) -> list(pid()).
%% @doc Pick out all pids of the peers
get_pids(ActiveV) ->
    lists:map(fun({_,Pid,_}) -> Pid end, ActiveV).

-spec remove_mrefs(active_view()) -> list(peer()).
%% @doc Strip away the monitor references when sending the peers to a process
remove_mrefs(ActiveV) ->
    [{Peer, Pid} || {Peer, Pid, _} <- ActiveV].

-spec add_active(active_peer(), active_view()) -> active_view().
%% @doc Just add an entry
add_active(PeerEntry, ActiveV) ->
    [PeerEntry|ActiveV].

-spec remove_active(pid(), active_view()) -> active_view();
                   (id(), active_view()) -> active_view().
%% @doc Remove an active entry by either pid or id
remove_active(Pid, ActiveV) when is_pid(Pid) ->
    lists:keydelete(Pid, 2, ActiveV);
remove_active(Peer, ActiveV) ->
    lists:keydelete(Peer, 1, ActiveV).

-spec peer_ok(id(), id(), active_view()) -> boolean().
%% @doc Check to see that a peer is ok, i.e it's not this node and not in the
%%      active view.
peer_ok(Peer, Myself, ActiveV) ->
    Peer =/= Myself andalso not in_active_view(Peer, ActiveV).

-spec in_active_view(id(), active_view()) -> boolean().
%% @doc See if a peer id is already in given active view.
in_active_view(Peer, ActiveV) ->
    lists:keymember(Peer, 1, ActiveV).

%%%===================================================================
%%% Passive view functions
%%%===================================================================

-spec add_passive_peer(id(), #st{}) -> #st{}.
%% @doc Add a peer to the passive view , removing random entries if needed.
add_passive_peer(Peer, S) ->
    #st{id=Myself, activev=ActiveV, passivev=PassiveV0, opts=Options} = S,
    case peer_ok(Peer, Myself, ActiveV, PassiveV0) of
        true ->
            Slots = length(PassiveV0)-gen_hypar_opts:passive_size(Options)+1,
            %% drop_n_random returns the same list if called with 0 or negative
            PassiveV1 = gen_hypar_util:drop_n_random(Slots, PassiveV0),
            PassiveV =  add_passive(Peer, PassiveV1),
            S#st{passivev=PassiveV};
        false ->
            S
    end.

-spec peer_ok(id(), id(), active_view(), passive_view()) -> boolean().
%% @doc Check that a peer is considered ok. That is, it's not this node or
%%      in any of the views.
peer_ok(Peer, Myself, ActiveV, PassiveV) ->
    peer_ok(Peer, Myself, ActiveV) andalso not lists:member(Peer, PassiveV).

-spec add_passive(id(), passive_view()) -> passive_view().
%% @doc Just add an element
add_passive(PeerId, PassiveV) ->
    [PeerId|PassiveV].

-spec remove_passive(id(), passive_view()) -> passive_view().
%% @doc Just a wrapper for lists:delete/2.
remove_passive(PeerId, PassiveV) ->
    lists:delete(PeerId, PassiveV).

%%%===================================================================
%%% Shuffle related functions
%%%===================================================================

-spec shuffle_timer(pos_integer()) -> ok.
%% @doc Start the shuffle timer
shuffle_timer(ShufflePeriod) ->
    erlang:send_after(ShufflePeriod, self(), shuffle), ok.

-spec add_xlist(#st{}, xlist(), xlist()) -> #st{}.
%% @doc Incorporate an exchange list into our passive view. If the passive view
%%      is full, first drop ids we recently sent away otherwise just drop random
%%      ids.
add_xlist(S, XList0, ReplyList) ->
    #st{id=Myself, activev=ActiveV, passivev=PassiveV0, opts=Options} = S,

    %% Filter out ourselves and nodes in active and passive view
    PeerOK = fun(Peer) -> peer_ok(Peer, Myself, ActiveV, PassiveV0) end,
    XList = lists:filter(PeerOK, XList0),
    
    %% If we need slots then first drop nodes from ReplyList then at random
    Slots = length(PassiveV0)-gen_hypar_opts:passive_size(Options)+length(XList),
    PassiveV = free_slots(Slots, PassiveV0, ReplyList),
    S#st{passivev=PassiveV++XList}.

-spec create_xlist(#st{}) -> xlist().
%% @doc Create an exchange list.
create_xlist(S) ->
    #st{id=Myself, activev=ActiveV0, passivev=PassiveV, opts=Options} = S,

    KActive  = gen_hypar_opts:k_active(Options),
    KPassive = gen_hypar_opts:k_passive(Options),

    RandomA = gen_hypar_util:take_n_random(KActive, get_ids(ActiveV0)),
    RandomP = gen_hypar_util:take_n_random(KPassive, PassiveV),
    [Myself | (RandomA ++ RandomP)].

%%%===================================================================
%%% Auxillary functions 
%%%===================================================================

-spec get_priority(active_view()) -> priority().
%% @doc Find the priority of a neighbour request. Empty is high, otherwise low.
get_priority([]) -> high;
get_priority(_)  -> low.

-spec free_slots(integer(), list(T), list(T)) -> list(T).
%% @doc Free up slots in a list. Negative inputs are just ignored and the same
%%      list is returned. Elements from the second list are dropped first then
%%      random.
free_slots(I, List, _) when I =< 0 ->
    List;
free_slots(I, List, []) ->
    gen_hypar_util:drop_n_random(I, List);
free_slots(I, List, [R|Rs]) ->
    case lists:member(R, List) of
        true  -> free_slots(I-1, lists:delete(R, List), Rs);
        false -> free_slots(I, List, Rs)
    end.

%%%===================================================================
%%% Register hypar node
%%%===================================================================

-spec register_hypar_node(id()) -> true.
%% @doc Register a hypar node
register_hypar_node(Identifier) ->    
    gen_hypar_util:register(name(Identifier)).

-spec wait_for(id()) -> {ok, pid()}.
%% @doc Wait for a the hypar node.
wait_for(Identifier) ->
    gen_hypar_util:wait_for(name(Identifier)).

-spec name(id()) -> {hypar_node, id()}.
%% @doc Gproc name of the hypar node
name(Identifier) ->
    {hypar_node, Identifier}.
