%% -------------------------------------------------------------------
%% Copyright (c) 2012 Emil Falk  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%%-------------------------------------------------------------------
%%% @author Emil Falk <emil.falk.1988@gmail.com>
%%% @copyright (C) 2012, Emil Falk
%%% @title HyParView node logic
%%% @doc This module implements the node logic in the HyParView-protocol
%%%-------------------------------------------------------------------
-module(hypar_node).

-behaviour(gen_server).

%%%%%%%%%%%%%
%% Imports %%
%%%%%%%%%%%%%
-include("hyparerl.hrl").

%%%%%%%%%%%%%
%% Exports %%
%%%%%%%%%%%%%

%% Operations
-export([start_link/1, stop/0, join_cluster/1, shuffle/0]).

%% View related
-export([get_peers/0, get_passive_peers/0]).

%% Events
-export([join/1, join_reply/1, forward_join/3, neighbour/2, disconnect/1,
         error/2, shuffle/4, shuffle_reply/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

%%%%%%%%%%%
%% State %%
%%%%%%%%%%%

-record(st, {id            :: id(),              %% This nodes identifier
             activev = []  :: active_view(),     %% The active view
             passivev = [] :: passive_view(),    %% The passive view
             last_xlist    :: xlist(),           %% The last shuffle xlist sent
             opts          :: options(),         %% Options
             connect_opts  :: options(),         %% Options related to TCP
             target        :: atom()             %% Target process that receives
            }).                                  %% link events and messages

%%%%%%%%%
%% API %%
%%%%%%%%%

%%%%%%%%%%%%%%%%
%% Operations %%
%%%%%%%%%%%%%%%%

-spec start_link(Options :: options()) ->
                        {ok, pid()} | ignore | {error, any()}.
%% @doc Start the <b>hypar_node</b> with <em>Options</em>.
start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

-spec stop() -> ok.
%% @doc Stop the <b>hypar_node</b>.
stop() ->
    gen_server:call(?MODULE, stop).

-spec join_cluster(ContactNode :: id()) -> ok | {error, any()}.
%% @doc Let the <b>hypar_node</b> join a cluster via <em>ContactNode</em>.
join_cluster(ContactNode) ->
    gen_server:call(?MODULE, {join_cluster, ContactNode}).

-spec shuffle() -> shuffle.
%% @doc Force a node to do a shuffle operation
shuffle() ->
    ?MODULE ! shuffle.

%%%%%%%%%%%%%%%%%%%%%
%% Peer operations %%
%%%%%%%%%%%%%%%%%%%%%

-spec get_peers() -> list({id(), pid()}).
%% @doc Get all the current active peers.
get_peers() ->
    gen_server:call(?MODULE, get_peers).

-spec get_passive_peers() -> passive_view().
%% @doc Get all the current passive peers.
get_passive_peers() ->
    gen_server:call(?MODULE, get_passive_peers).

%%%%%%%%%%%%
%% Events %%
%%%%%%%%%%%%

-spec join(Sender :: id()) -> ok | {error, already_in_active}.
%% @doc Join <em>Sender</em>.
join(Sender) ->
    gen_server:call(?MODULE, {join, Sender}).

-spec forward_join(Sender :: id(), NewNode :: id(), TTL :: non_neg_integer()) ->
                          ok.
%% @doc Forward join <em>NewNode</em> from <em>Sender</em> with time to live
%%      <em>TTL</em>.
forward_join(Sender, NewNode, TTL) ->
    gen_server:cast(?MODULE, {forward_join, Sender, NewNode, TTL}).

-spec join_reply(Sender :: id()) -> ok | {error, already_in_active}.
%% @doc Join reply from <em>Sender</em>.
join_reply(Sender) ->
    gen_server:call(?MODULE, {join_reply, Sender}).

-spec neighbour(Sender :: id(), Priority :: priority()) ->
                       accept | decline | {error, already_in_active}.
%% @doc Neighbour request from <em>Sender</em> with <em>Priority</em>.
neighbour(Sender, Priority) ->
    gen_server:call(?MODULE, {neighbour, Sender, Priority}).

-spec disconnect(Sender :: id()) -> ok | {error, not_in_active}.
%% @doc Disconnect <em>Sender</em>.
disconnect(Sender) ->
    gen_server:cast(?MODULE, {disconnect, Sender}).

-spec error(Sender :: id(), Reason :: any()) -> ok | {error, not_in_active}.
%% @doc Let the <b>hypar_node</b> know that <em>Sender</em> has failed
%%      with <em>Reason</em>.
error(Sender, Reason) ->
    gen_server:cast(?MODULE, {error, Sender, Reason}).

-spec shuffle(Sender :: id(), Requester :: id(), TTL :: non_neg_integer(),
              XList :: xlist()) -> ok.
%% @doc Shuffle request from <em>Sender</em>. The shuffle request originated in
%%      node <em>Requester</em> and <em>XList</em> contains sample node
%%      identifiers. The message has a time to live of <em>TTL</em>
shuffle(Sender, Requester, TTL, XList) ->
    gen_server:cast(?MODULE, {shuffle, Sender, Requester, TTL, XList}).

-spec shuffle_reply(ReplyXList :: xlist()) -> ok.
%% @doc Shuffle reply to shuffle request with reference <em>Ref</em> sent from
%%      <em>Sender</em> that carries the sample list <em>ReplyXList</em>.
shuffle_reply(ReplyXList) ->
    gen_server:cast(?MODULE, {shuffle_reply, ReplyXList}).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(Options :: options()) -> {ok, #st{}}.
%% Initialize the hypar_node
init(Options) ->
    %% Seed the random generator!
    random:seed(now()),

    %% Find this nodes id
    ThisNode = proplists:get_value(id, Options),
    lager:info([{options, Options}], "Initializing..."),
    
    %% Find target process
    Target = proplists:get_value(target, Options),    

    ConnectOpts = connect:initialize(Options),

    %% Start shuffle
    ShufflePeriod = proplists:get_value(shuffle_period, Options),
    shuffle_timer(ShufflePeriod),
    
    {ok, #st{id=ThisNode,
             opts=Options,
             connect_opts=ConnectOpts,
             target=Target}}.

%% Join a cluster via a given contact-node
%% According to the paper this should only be done once. I don't really see
%% why one would not be able to do multiple cluster joins to rejoin or to
%% populate the active view faster
handle_call({join_cluster, ContactNode}, _, S0) ->
    case connect:join(ContactNode, S0#st.connect_opts) of
        {ok, P} -> {reply, ok, add_node_active(P, S0)};
        {error, Err} ->
            lager:error("Join cluster via ~p failed with error ~p.~n",
                        [ContactNode, Err]),
            {reply, {error, Err}, S0}
    end;

%% Add newly joined node to active view, propagate forward joins
handle_call({join, Sender}, {Pid,_}, S0) ->
    S = add_node_active(#peer{id=Sender, conn=Pid}, S0),
    
    %% Send forward joins
    ARWL = proplists:get_value(arwl, S#st.opts),
    
    ForwardFun = fun(P) -> connect:forward_join(P, Sender, ARWL) end,
    FilterFun  = fun(P) -> P#peer.id =/= Sender end,
    ForwardNodes = lists:filter(FilterFun, S#st.activev),
    
    lists:foreach(ForwardFun, ForwardNodes),
    
    {reply, ok, S};

%% Accept a connection from the join procedure
handle_call({join_reply, Sender}, {Pid,_} , S0) ->
    {reply, ok, add_node_active(#peer{id=Sender, conn=Pid}, S0)};

%% Neighbour request, either accept or decline based on priority and current
%% active view
handle_call({neighbour, Sender, Priority},{Pid,_} , S) ->
    P = #peer{id=Sender, conn=Pid},
    case Priority of
        %% High priority neighbour request thus the node needs to accept
        %% the request what ever the current active view is
        high ->
            lager:info("Neighbour accepted: ~p.~n", [P]),
            {reply, accept, add_node_active(P, S)};
        %% Low priority request, only accept if we have space in the
        %% nodes active view
        low ->
            ASize = proplists:get_value(active_size, S#st.opts),
            case length(S#st.activev) < ASize of
                true  -> {reply, accept, add_node_active(P, S)};
                false -> lager:info("Neighbour declined ~p.~n", [P]),
                         {reply, decline, S}
            end
    end;

%% Return current active peers
handle_call(get_peers, _, S) ->
    Active = [{P#peer.id, P#peer.conn} || P <- S#st.activev],
    {reply, Active, S};

%% Return current passive peers
handle_call(get_passive_peers, _, S) ->
    {reply, S#st.passivev, S};

%% Stop the hypar_node
handle_call(stop, _, S) ->
    connect:stop(),
    {stop, normal, ok, S}.

%% Respond to a forward_join, add to active or propagate and maybe add to
%% passive view.
handle_cast({forward_join, Sender, NewNode, TTL}, S0) ->
    case TTL =:= 0 orelse length(S0#st.activev) =:= 1 of
        true ->
            %% Add to active view, send a reply to the join_reply to let the
            %% other node know
            case connect:join_reply(NewNode, S0#st.connect_opts) of
                {ok, P} -> {noreply, add_node_active(P, S0)};
                {error, Err} ->
                    lager:error("Join reply error ~p to ~p.~n", [Err, NewNode]),
                    {noreply, S0}
            end;
        false ->
            %% Add to passive view if TTL is equal to PRWL
            PRWL = proplists:get_value(prwl, S0#st.opts),
            S1 = case TTL =:= PRWL of
                     true  -> add_node_passive(NewNode, S0);
                     false -> S0
                 end,

            %% Propagate the forward join using a random walk
            AllButSender = lists:keydelete(Sender, #peer.id, S1#st.activev),
            P = misc:random_elem(AllButSender),

            connect:forward_join(P, NewNode, TTL-1),
            {noreply, S1}
    end;

%% Respond to a shuffle request, either propagate it or accept it via a
%% temporary connection to the source of the request. If the node accept then
%% it adds the shuffle list into it's passive view and responds with with
%% a shuffle reply
handle_cast({shuffle, Sender, Req, TTL, XList}, S) ->
    case TTL > 0 andalso length(S#st.activev) > 1 of
        %% Propagate the random walk
        true ->
            AllButSender = lists:keydelete(Sender, #peer.id, S#st.activev),
            P = misc:random_elem(AllButSender),
            connect:shuffle(P, Req, TTL-1, XList),
            {noreply, S};
        %% Accept the shuffle request, add to passive view and reply
        false ->
            ReplyXList = misc:take_n_random(length(XList), S#st.passivev),
            connect:shuffle_reply(Req, ReplyXList, S#st.connect_opts),
            {noreply, add_xlist(S, XList, ReplyXList)}
    end;

%% Accept a shuffle reply, add the reply list into the passive view and
%% close the temporary connection.
handle_cast({shuffle_reply, ReplyXList}, S0) ->
    S = S0#st{last_xlist=[]},
    {noreply, add_xlist(S, ReplyXList, S0#st.last_xlist)};

%% Disconnect an open active connection, add disconnecting node to passive view
handle_cast({disconnect, Sender}, S0) ->
    %% Disconnect the peer, close the connection and add node to passive view
    ActiveV = lists:keydelete(Sender, #peer.id, S0#st.activev),
    {noreply, add_node_passive(Sender, S0#st{activev=ActiveV})};


%% Handle failing connections. Try to find a new one if possible
handle_cast({error, Sender, Reason}, S0) ->
    lager:error("Active link to ~p failed with error ~p.~n",
                [Sender, Reason]),
    S = S0#st{activev=lists:keydelete(Sender, #peer.id, S0#st.activev)},
    {noreply, find_new_active(S)}.

%% Timer message for periodic shuffle. Send of a shuffle request to a random
%% peer in the active view. Ignore if we don't have any active connections.
handle_info(shuffle, S) ->
    LastXList = 
        case S#st.activev =:= [] of
            true -> [];
            false ->
                XList = create_xlist(S),
                P = misc:random_elem(S#st.activev),                
                ARWL = proplists:get_value(arwl, S#st.opts),
                
                connect:shuffle(P, S#st.id, ARWL-1, XList),
                XList
        end,
    
    ShufflePeriod = proplists:get_value(shuffle_period, S#st.opts),
    shuffle_timer(ShufflePeriod),

    {noreply, S#st{last_xlist=LastXList}}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_, _) ->
    connect:stop().

%%%%%%%%%%%%%%%%%%%%%
%% Shuffle related %%
%%%%%%%%%%%%%%%%%%%%%

-spec shuffle_timer(ShufflePeriod :: non_neg_integer()) -> ok;
                   (undefined) -> ok.
%% @private
%% @doc Set the shuffle timer to <em>ShufflePeriod</em>. Or if undefined
%%      this is a no-op that returns ok.
shuffle_timer(ShufflePeriod) ->
    case ShufflePeriod of
        undefined ->
            ok;
        ShufflePeriod ->
            erlang:send_after(ShufflePeriod, self(), shuffle),
            ok
    end.

-spec add_xlist(S :: #st{}, XList0 :: xlist(), ReplyList :: xlist()) -> #st{}.
%% @private
%% @doc Add <em>XList0</em> it into the view in state <em>S</em>. Do not add
%%      nodes that are already in active or passive view from the list. If the
%%      passive view are full, start by dropping elements from ReplyList then random
%%      elements.
add_xlist(S, XList0, ReplyList) ->
    PassiveSize = proplists:get_value(passive_size, S#st.opts),
    ActiveV = S#st.activev,
    PassiveV0 = S#st.passivev,
    Id = S#st.id,
    Filter = fun(Node) -> node_ok(Node, Id, ActiveV, PassiveV0) end,
    XList = lists:filter(Filter, XList0),
    PassiveV = free_slots(length(PassiveV0)-PassiveSize+length(XList),
                          PassiveV0, ReplyList),
    S#st{passivev=PassiveV++XList}.

-spec create_xlist(S :: #st{}) -> xlist().
%% @private
%% @doc Create the exchange list in state <em>S</em> used in a shuffle request.
create_xlist(S) ->
    KActive = proplists:get_value(k_active, S#st.opts),
    KPassive = proplists:get_value(k_passive, S#st.opts),

    ActiveV = lists:map(fun(P) -> P#peer.id end, S#st.activev),

    RandomActive  = misc:take_n_random(KActive, ActiveV),
    RandomPassive = misc:take_n_random(KPassive, S#st.passivev),
    [S#st.id | (RandomActive ++ RandomPassive)].

%%%%%%%%%%%%%%%%%%%%%%%%%
%% Active view related %%
%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Notify <em>Target</em> of a <b>link_up</b> event to node <em>To</em>.
link_up(Target, To, Conn) ->
    lager:info("Link up: ~p~n", [{To, Conn}]),
    gen_server:cast(Target, {link_up, To, Conn}).

-spec add_node_active(Peer :: #peer{}, S0 :: #st{}) -> #st{}.
%% @private
%% @doc Add <em>Peer</em> to the active view in state <em>S0</em>, removing a
%%      node if necessary. The new state is returned. If a node has to be
%%      dropped, then it is informed via a DISCONNECT message and placed in the
%%      passive view.
add_node_active(Peer, S0) ->
    Id = Peer#peer.id,
    ActiveV0 = S0#st.activev,
    case Id =/= S0#st.id andalso not lists:keymember(Id, #peer.id, ActiveV0) of
        true ->
            ASize = proplists:get_value(active_size, S0#st.opts),
            S = case length(ActiveV0) >= ASize of
                    true  -> drop_random_active(S0);
                    false -> S0
                end,
            link_up(S#st.target, Id, Peer#peer.conn),
            S#st{activev=[Peer|S#st.activev],
                 %% Make sure peer are not in both view.
                 passivev=lists:delete(Peer#peer.id, S#st.passivev)};
        false ->
            S0
    end.

-spec drop_random_active(S :: #st{}) -> #st{}.
%% @private
%% @doc Drop a random node from the active view in state down to the passive
%%      view in <em>S</em>. Send a disconnect message to the dropped node.
drop_random_active(S) ->
    PassiveSize = proplists:get_value(passive_size, S#st.opts),
    PassiveV0 = S#st.passivev,
    PassiveL = length(PassiveV0),
    Slots = PassiveL-PassiveSize+1,
    {Peer, ActiveV} = misc:drop_random(S#st.activev),
    PassiveV = [Peer#peer.id|misc:drop_n_random(Slots, PassiveV0)],

    connect:disconnect(Peer),
    
    S#st{activev=ActiveV, passivev=PassiveV}.

-spec find_new_active(S :: #st{}) -> #st{}.
%% @private
%% @doc Find a new active peer in state <em>S</em>. The function will send
%%      neighbour requests to nodes in passive view until it finds a good one.
find_new_active(S) ->
    Priority = get_priority(S#st.activev),

    case find_neighbour(Priority, S#st.passivev, S#st.connect_opts) of
        {no_valid, PassiveV} ->
            lager:info("No accepting peers in passive view.~n"),
            S#st{passivev=PassiveV};
        {Peer, PassiveV} ->
            add_node_active(Peer, S#st{passivev=PassiveV})
    end.

-spec find_neighbour(Priority :: priority(), PassiveV :: passive_view(),
                    ConnectArgs :: options()) -> {#peer{} | no_valid, passive_view()}.
%% @private
%% @doc Try to find a new active neighbour to <em>ThisNode</em> with priority
%%      <em>Priority</em>. Try random nodes out of <em>PassiveV</em>, removing
%%      failing once and logging declined requests. Returns either a new active
%%      peer along with the new passive view or <b>no_valid</b> if no peers
%%      were connectable.
find_neighbour(Priority, PassiveV, ConnectOpts) ->
    find_neighbour(Priority, PassiveV, ConnectOpts, []).

-spec find_neighbour(Priority :: priority(), PassiveV :: passive_view(),
                     ConnectOpts :: options(), Tried :: view()) ->
                            {#peer{} | no_valid, passive_view()}.
%% @private
%% @doc Helper function for find_neighbour/3.
find_neighbour(_, [], _, Tried) ->
    {no_valid, Tried};
find_neighbour(Priority, PassiveV0, ConnectOpts, Tried) ->
    {Node, Passive} = misc:drop_random(PassiveV0),
    case connect:neighbour(Node, Priority, ConnectOpts) of
        {ok, P} -> {P,Passive ++ Tried};
        decline -> find_neighbour(Priority, Passive, ConnectOpts, [Node|Tried]);
        {error, Err} ->
            lager:error("Neighbour error ~p to ~p.~n", [Err, Node]),
            find_neighbour(Priority, Passive, ConnectOpts, Tried) 
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Passive view related %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_node_passive(Node :: id(), S :: #st{}) -> #st{}.
%% @private
%% @doc Add <em>Node</em> to the passive view in state <em>S</em>, removing
%%      random entries if needed.
add_node_passive(Node, S) ->
    ActiveV = S#st.activev,
    PassiveV0 = S#st.passivev,
    Id = S#st.id,
    case node_ok(Node, Id, ActiveV, PassiveV0) of
        true ->
            PSize = proplists:get_value(passive_size, S#st.opts),
            Slots = length(PassiveV0)-PSize+1,
            %% drop_n_random returns the same list if called with 0 or negative
            PassiveV =  [Node|misc:drop_n_random(Slots, PassiveV0)],
            S#st{passivev=PassiveV};
        false ->
            S
    end.

node_ok(Node, Id, ActiveV, PassiveV) ->
    Node =/= Id andalso not lists:keymember(Node, #peer.id, ActiveV) andalso
        not lists:member(Node, PassiveV).

%%%%%%%%%%%%%%%
%% Pure code %%
%%%%%%%%%%%%%%%

-spec get_priority(list(#peer{})) -> priority().
%% @pure
%% @private
%% @doc Find the priority of a neighbour. If no active entries exist
%%      the priority is <b>high</b>, otherwise <b>low</b>.
get_priority([]) -> high;
get_priority(_)  -> low.

-spec free_slots(I :: integer(), List :: list(T), Rs :: list(T)) -> list(T).
%% @pure
%% @private
%% @doc Free up <em>I</em> slots in <em>List</em>, start by removing elements
%%      from <em>Rs</em>, then remove at random.
free_slots(I, List, _) when I =< 0 -> List;
free_slots(I, List, []) -> misc:drop_n_random(I, List);
free_slots(I, List, [R|Rs]) ->
    case lists:member(R, List) of
        true  -> free_slots(I-1, lists:delete(R, List), Rs);
        false -> free_slots(I, List, Rs)
    end.
