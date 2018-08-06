-module(test_mod).
-behaviour(gen_hypar).
-behaviour(gen_server).

-export([test/1, join_them/1, join/2, get_peers/1, get_hyparnode_peers/1]).

-export([start/1, bcast/2, start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3,
         terminate/2]).

test(N) ->
    application:start(ranch),
    application:start(gproc),
    [start(local_id(5000+I)) || I <- lists:seq(0,N)].

join_them(N) ->
    [join(5000+I, 5000) || I <- lists:seq(1,N)].

local_id(Port) ->
    {{127,0,0,1}, Port}.

start(Identifier) ->
    gen_hypar:start_link(Identifier, ?MODULE, [], []).

start_link(Identifier, _) ->
    gen_server:start_link(?MODULE, [Identifier], []).

join(Port, Contact) ->
    gen_server:call(gen_hypar:where(local_id(Port)),
                    {join, local_id(Contact)}).

get_peers(Port) ->
    gen_server:call(gen_hypar:where(local_id(Port)),
                    get_peers).

get_hyparnode_peers(Port) ->
    gen_server:call(gen_hypar:where(local_id(Port)),
                    get_hyparnode_peers).

bcast(Port, Msg) ->
    gen_server:cast(gen_hypar:where(local_id(Port)), {bcast, Msg}).

init([Identifier]) ->
    {ok, HyparNode} = gen_hypar:register_and_wait(Identifier),
    {ok, {Identifier, HyparNode, []}}.

handle_call({join, Contact},_ ,{_, HyparNode, _}=S) ->
    {reply, gen_hypar:join_cluster(HyparNode, Contact), S};
handle_call(get_peers, _, {_,_,Peers}=S) ->
    {reply, Peers, S};
handle_call(get_hyparnode_peers, _, {_,HyparNode, _}=S) ->
    {reply, gen_hypar:get_peers(HyparNode), S}.

handle_cast({bcast, Msg}, {_, _, Peers}=S) ->
    broadcast(Msg, Peers),
    {noreply, S}.

handle_info({message, From, Msg}, {Id, _, _}=S) ->
    io:format("NODE:~p From:~p Message:~p~n", [Id, From, Msg]),
    {noreply, S};
handle_info({link_up, {Peer, Pid}=E}, {Id, HyparNode, Peers}) ->
    io:format("NODE: ~p Link_up:~p Pid:~p~n", [Id, Peer, Pid]),
    {noreply, {Id, HyparNode, [E|Peers]}};
handle_info({link_down, Peer}, {Id, HyparNode, Peers}) ->
    io:format("NODE: ~p Link down:~p~n", [Id, Peer]),
    {noreply, {Id, HyparNode, lists:keydelete(Peer, 1, Peers)}}.

code_change(_,S,_) ->
    {ok, S}.

terminate(_, _) ->
    ok.

broadcast(Msg, Peers) ->    
    [gen_hypar:send_message(Pid, Msg) || {_, Pid} <- Peers].
