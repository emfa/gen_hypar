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
%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Connection module
%% @doc This module implements all code related to open, close and sending
%%      data over connections in hyparerl. 
%% -------------------------------------------------------------------

%% @doc =Binary formats=
%%
%%      ==Identifier=
%%      encode_id({{A, B, C, D}, Port}) = <<A, B, C, D, Port:16/integer>>
%%
%%      ==Join==
%%      <<?JOIN, BId:6/binary>>
%%
%%      ==Forward join== 
%%      <<?FORWARDJOIN, BReq:6/binary, TTL/integer>>
%%
%%      ==Join reply==
%%      <<?JOINREPLY, BId:6/binary>>
%%
%%      ==High priority neighbour request==
%%      <<?HNEIGHBOUR, BId:6/binary>>
%%
%%      ==Low priority neighbour request==
%%      <<?LNEIGHBOUR, BId:6/binary>>
%%
%%      ==Neighbour accept/decline==
%%      Accept:  <<?ACCEPT>>
%%      Decline: <<?DECLINE>>
%%
%%      ==Disconnect==
%%      <<?DISCONNECT>>
%%
%%      ==Shuffle==
%%      <<?SHUFFLE, BReq:6/binary, TTL/integer, Len/integer, XList/binary>>
%%
%%      ==Shuffle reply==
%%      <<?SHUFFLEREPLY, Len/integer, XList/binary>>
%%
%% @todo
%%      Because of the format of the protocol some configuration parameters
%%      have become restricted. I do not think that it imposes any problem.
%%      Should implement a sanity check on the options when starting the
%%      application. For example so that Len or TTL never overflows.
-module(connect).

-behaviour(ranch_protocol).
-behaviour(gen_fsm).

%% Start via either connect_sup or ranch
-export([start_link/3, start_link/4]).

%% Initialize, send and filter options
-export([initialize/1, send/2]).

%% Called by hypar_node
-export([join/2, forward_join/3, join_reply/2, neighbour/3, disconnect/1,
         shuffle/4, shuffle_reply/3]).

%% De-/Serialization of identifiers
-export([encode_id/1, decode_id/1]).

%% States
-export([wait_incoming/2, active/2, active/3, wait_for_socket/2, wait_for_ranch/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-include("hyparerl.hrl").
-include("connect.hrl").

%% Connection state
-record(conn, {remote  :: #peer{},       %% Identifier of the remote end
               socket  :: inet:socket(), %% The socket
               data    :: binary(),      %% Incoming receive-buffer
               mod     :: module(),      %% Callback module
               timeout :: timeout(),     %% Timeout
               send_timeout :: timeout() %% Send timeout
              }).

%%%===================================================================
%%% Operation
%%%===================================================================

-spec initialize(Options :: options()) -> options().
%% @doc Start the ranch listener with <em>Options</em>.
%% @todo Unsure if I should start the ranch listener here.
initialize(Options) ->
    {Ip, Port0} =  proplists:get_value(id, Options, get_ip_and_port(Options)),
    ListenOpts = [{ip, Ip}, {port,Port0}],
    {ok, _} = ranch:start_listener(hyparerl, 20, ranch_tcp, ListenOpts,
                                   connect, opts:connect_opts(Options)),    
    Port = ranch:get_port(hyparerl),
    [{id, {Ip, Port}}|opts:connect_opts(Options)].

%% @doc Send a binary message <em>Message</em> to <em>Conn</em>.
send(Conn, Message) -> gen_fsm:send_event(Conn, {message, Message}).

%% @doc Start an incoming connection via ranch
start_link(LPid, Socket, _Transport, ConnectOpts) ->
    gen_fsm:start_link(?MODULE, [incoming, LPid, Socket, ConnectOpts], []).

%% @doc Start an outgoing active connection
start_link(ConnectOpts, Remote, Socket) ->
    gen_fsm:start_link(?MODULE, [outgoing, Remote, Socket, ConnectOpts], []).

%%%===================================================================
%%% Hypar_node functions
%%%===================================================================

%% @doc Create a tcp-connection to <em>To</em>. Send a join message and put this
%%      connection in active state.
join(Remote, Opts) ->
<<<<<<< HEAD
    BId = encode_id(opts:myself(Opts)),
    connect_send_active(Remote, Opts, <<?JOIN, BId/binary>>).
=======
    catch send_join(Remote, Opts).
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Send a forward join over an existing active connection <em>Conn</em>.
forward_join(Peer, Req, TTL) ->
    gen_fsm:send_event(Peer#peer.conn, {forward_join, Req, TTL}).

%% @doc Create a new active connection <em>To</em> and send a join_reply.
join_reply(Remote, Opts) ->
<<<<<<< HEAD
    BId = encode_id(opts:myself(Opts)),
    connect_send_active(Remote, Opts, <<?JOINREPLY, BId/binary>>).
=======
    catch send_join_reply(Remote, Opts).
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Create a new connection to <em>To</em> and send a neighbour request with
%%      priority <em>Priority</em>. 
neighbour(Remote, Priority, Opts) ->
<<<<<<< HEAD
    send_neighbour_request(Remote, Priority, Opts).
=======
    catch send_neighbour_request(Remote, Priority, Opts).
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Send a shuffle request to an active connection <em>Conn</em>. The
%%      source node is <em>Req</em> with id <em>SId</em>, time to live
%%      <em>TTL</em> and exchange list <em>XList</em>.
shuffle(Peer, Req, TTL, XList) ->
    gen_fsm:send_event(Peer#peer.conn, {shuffle, Req, TTL, XList}).

%% @doc Send a shuffle reply to <em>To</em> in shuffle reply with id
%%      <em>SId</em> carrying the reply list <em>XList</em>.
shuffle_reply(Remote, XList, Opts) ->
<<<<<<< HEAD
    send_shuffle_reply(Remote, XList, Opts).
=======
    catch send_shuffle_reply(Remote, XList, Opts).
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Disconnect an active connection <em>Conn</em>.
disconnect(Peer) -> gen_fsm:sync_send_event(Peer#peer.conn, disconnect).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([incoming, LPid, Socket, Opts]) ->
    {ok, wait_for_ranch, {LPid, initial_state(Socket, Opts), Opts}, 0};

init([outgoing, Remote, Socket, Options]) ->
    C = initial_state(Socket, Options),
    {ok, wait_for_socket, C#conn{remote=peer(Remote)}}.

%% Receive socket data, parse and take appropriate action
handle_info({tcp, Socket, Data}, active, C) ->
    socket_active(Socket),
    RestBin = C#conn.data,
    parse_packets(C, <<RestBin/binary, Data/binary>>);

%% An active connection has been closed or experienced an error, report it
handle_info({tcp_closed, _}, active, C) ->
    link_down(C#conn.mod, C#conn.remote),
    hypar_node:error(C#conn.remote, tcp_closed),
    {stop, normal, C};
handle_info({tcp_error, _, Reason}, active, C) ->
    link_down(C#conn.mod, C#conn.remote),
    hypar_node:error(C#conn.remote, Reason),
    {stop, normal, C};

%% A temporary connection has been closed or failed, ignore and stop
handle_info({tcp_closed, _}, temporary, C) ->
    {stop, normal, C};
handle_info({tcp_error, _, _}, temporary, C) ->
    {stop, normal, C}.

handle_event(_, _, S) ->
    {stop, not_used, S}.

handle_sync_event(_, _, _, S) ->
    {stop, not_used, S}.

code_change(_,StateName, StateData, _) ->
    {ok, StateName, StateData}.

terminate(_, _, _) ->
    ok.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Active state
%%      Four possibilities; Either it is a binary message, a forward-join,
%%      a shuffle or a disconnect.
active({message, Message}, C) ->
    Len = byte_size(Message),
    try_send(C, [?MESSAGE, <<Len:32/integer>>, Message]);

active({forward_join, Req, TTL}, C) ->
    BReq = encode_id(Req),
    try_send(C, <<?FORWARDJOIN, BReq:6/binary, TTL/integer>>);

active({shuffle, Req, TTL, XList}, C) ->
    Len = length(XList),
    BReq = encode_id(Req),
    BXList = encode_xlist(XList),
    Bin = <<?SHUFFLE, BReq:6/binary, TTL/integer, Len/integer, BXList/binary>>,
    try_send(C, Bin);
<<<<<<< HEAD
    
=======
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275
active(timeout, C) ->
    link_up(C#conn.mod, C#conn.remote),
    socket_active(C#conn.socket),
    {next_state, active, C}.

active(disconnect, _, C) ->
    Socket = C#conn.socket,
    link_down(C#conn.mod, C#conn.remote),
    gen_tcp:send(Socket, <<?DISCONNECT>>),
    gen_tcp:close(Socket),
    {stop, normal, ok, C}.

%% @doc Accept an incoming connection on <em>Socket</em>.
%%      To accept a connection we first read one byte to check what the first
%%      message in is. This can be either:
%%      A join, forward join reply, low/high neighbour request or a shuffle
%%      reply. 
wait_incoming(timeout, {C, Opts}) ->
    Socket = C#conn.socket,
    SendTimeout = proplists:get_value(send_timeout, Opts),
    Timeout = proplists:get_value(timeout, Opts),
    inet:setopts(Socket, [{send_timeout, SendTimeout}]),
    %% Read 1 bytes to determine what kind of message it is
    {ok, Bin} = gen_tcp:recv(Socket, 1 , Timeout),
    case Bin of
        <<?JOIN>>         -> handle_join(C, Timeout);
        <<?JOINREPLY>>    -> handle_join_reply(C, Timeout);
        <<?LNEIGHBOUR>>   -> handle_neighbour(C, Timeout, low);
        <<?HNEIGHBOUR>>   -> handle_neighbour(C, Timeout, high);
        <<?SHUFFLEREPLY>> -> handle_shuffle_reply(C, Timeout)
    end.

%% Wait for the go-ahead from the hypar_node starting the connection
wait_for_socket({go_ahead, Socket}, C=#conn{socket=Socket}) ->
    {next_state, active, C, 0}.

%% Wait for the go-ahead from the ranch server
wait_for_ranch(timeout, {LPid, C, Opts}) ->
    ok = ranch:accept_ack(LPid),
    {next_state, wait_incoming, {C, Opts}, 0}.

%%%===================================================================
%%% Related to outgoing connections
%%%===================================================================

<<<<<<< HEAD
%% @doc Try to connect and send Bin. If successful then move the connection
%%      to active state.
connect_send_active(Remote, Opts, Bin) ->
    case start_connection(Remote, Opts) of
        {ok, Socket} ->
            case gen_tcp:send(Socket, Bin) of
                ok ->  {ok, new_peer(Remote, Socket)};
                Err -> Err
            end;
        Err -> Err
    end.
=======
%% @doc Try to send a join over the socket. If successful returns a peer or
%%      an error.
send_join(Remote, Opts) ->
    {ok, Socket} = start_connection(Remote, Opts),
    BId = encode_id(opts:myself(Opts)),
    ok = gen_tcp:send(Socket, <<?JOIN, BId/binary>>),
    {ok, new_peer(Remote, Socket)}.

%% @doc Try to send a join reply over the socket, return a peer if sucessful.
send_join_reply(Remote, Opts) ->
    {ok, Socket} = start_connection(Remote, Opts),
    BId = encode_id(opts:myself(Opts)),
    ok = gen_tcp:send(Socket, <<?JOINREPLY, BId/binary>>),
    {ok, new_peer(Remote, Socket)}.
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Try to send a neighbour request over the socket. Wait for a reply,
%%      and respond to that reply. If the request is successful return a peer.
send_neighbour_request(Remote, Priority, Opts) ->
<<<<<<< HEAD
    case start_connection(Remote, Opts) of
        {ok, Socket} ->
            Timeout = opts:timeout(Opts),
            BId = encode_id(opts:myself(Opts)),
            Bin = case Priority of
                      high -> <<?HNEIGHBOUR, BId/binary>>;
                      low  -> <<?LNEIGHBOUR, BId/binary>>
                  end,
            case gen_tcp:send(Socket, Bin) of
                ok ->  wait_for_neighbour_reply(Remote, Socket, Timeout);
                Err -> Err
            end;
        Err -> Err
    end.
=======
    {ok, Socket} = start_connection(Remote, Opts),
    Timeout = opts:timeout(Opts),
    BId = encode_id(opts:myself(Opts)),
    Bin = case Priority of
              high -> <<?HNEIGHBOUR, BId/binary>>;
              low  -> <<?LNEIGHBOUR, BId/binary>>
          end,
    ok = gen_tcp:send(Socket, Bin),
    wait_for_neighbour_reply(Remote, Socket, Timeout).
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Receive an accept/decline byte from the socket.
wait_for_neighbour_reply(Remote, Socket, Timeout) ->
    case gen_tcp:recv(Socket, 1, Timeout) of
        {ok, <<?ACCEPT>>} ->
            {ok, new_peer(Remote, Socket)};
        {ok, <<?DECLINE>>} ->
            gen_tcp:close(Socket),
<<<<<<< HEAD
            decline;
        Err ->
            Err
=======
            decline
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275
    end.

%% @doc Send a shuffle reply
send_shuffle_reply(Remote, XList, Opts) ->
<<<<<<< HEAD
    case start_connection(Remote, Opts) of
        {ok, Socket} ->
            Len = length(XList),
            BXList = encode_xlist(XList),
            Bin = <<?SHUFFLEREPLY, Len/integer, BXList/binary>>,
            ok = gen_tcp:send(Socket, Bin),
            gen_tcp:close(Socket),    
            ok;
        _Err ->
            ok
    end.
=======
    {ok, Socket} = start_connection(Remote, Opts),
    Len = length(XList),
    BXList = encode_xlist(XList),
    Bin = <<?SHUFFLEREPLY, Len/integer, BXList/binary>>,
    ok = gen_tcp:send(Socket, Bin),    
    gen_tcp:close(Socket),    
    ok.
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275

%% @doc Create a new peer, change controlling process and send a go-ahead
%%      to the new connection process.
new_peer(Remote, Socket) ->
    {ok, Pid} = supervisor:start_child(connect_sup, [Remote, Socket]),
    gen_tcp:controlling_process(Socket, Pid),
    gen_fsm:send_event(Pid, {go_ahead, Socket}),
    #peer{id=Remote, conn=Pid}.

%%%===================================================================
%%% Related to incoming connections
%%%===================================================================

%% @doc Handle a incoming join. Receive the identifier and
%%      call hypar_node:join(Id) and move to active.
handle_join(C, Timeout) ->
    {ok, Id} = receive_id(C, Timeout),
    Remote = peer(Id),
    ok = hypar_node:join(Remote),
    {next_state, active, C#conn{remote=Remote}, 0}.

%% @doc Handle an incoming join reply.
%%      Call hypar_node:join_reply(Id) and move to active
handle_join_reply(C, Timeout) -> 
    {ok, Id} = receive_id(C, Timeout),
    Remote = peer(Id),
    ok = hypar_node:join_reply(Remote),
    {next_state, active, C#conn{remote=Remote}, 0}.

%% @doc Handle an incoming neighbour request. Construct the remote
%%      Id and call hypar_node:neighbour(Id, Priority) checking the return and
%%      either send ACCEPT or DECLINE back on the socket.
handle_neighbour(C0, Timeout, Priority) ->
    {ok, Id} = receive_id(C0, Timeout),
    Remote = peer(Id),
    C = C0#conn{remote=Remote},
<<<<<<< HEAD
    case hypar_node:neighbour(Remote, Priority) of
=======
    case hypar_node:neighbour(Id, Priority) of
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275
        accept  -> accept_neighbour(C);
        decline -> decline_neighbour(C)
    end.

%% @doc Accept a neighbour, send ACCEPT back on socket and move to active.
accept_neighbour(C) ->
    case gen_tcp:send(C#conn.socket, <<?ACCEPT>>) of
        ok ->
            {next_state, active, C, 0};
        {error, Err} ->
            ok = hypar_node:error(C#conn.remote, Err),
            {stop, {error, Err}, C}
    end.

%% @doc Decline a neighbour, send DECLINE back on socket and close this
%%      connection.
decline_neighbour(C) ->
    Socket = C#conn.socket,
    ok = gen_tcp:send(Socket, <<?DECLINE>>),
    {next_state, temporary, C}.

%% @doc Receive the exchange list of length <em>Len</em>  for shuffle with id
%%      <em>SId</em> for the shuffle reply.
handle_shuffle_reply(C, Timeout) ->
    Socket = C#conn.socket,
    {ok, <<Len>>} = gen_tcp:recv(Socket, 1, Timeout),
    case Len of
        0 ->
            ok = hypar_node:shuffle_reply([]);
        _ ->
            {ok, Bin} = gen_tcp:recv(Socket, Len*6, Timeout),
            ok = hypar_node:shuffle_reply(parse_xlist(Bin))
    end,
    gen_tcp:close(Socket),
    {stop, normal, C}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec deliver(Mod :: module(), Sender :: id(), Bin :: binary()) -> ok.
%% @doc Deliver a message <em>Bin</em> to callback <em>Mod</em> from connection
%%      <em>Sender</em>.
deliver(Mod, Sender, Bin) ->
    ok = Mod:deliver(Sender, Bin).

%% @doc Notify callback module <em>Mod</em> of a link_up event.
link_up(Mod, Peer) ->
    lager:info("Link up: ~p~n", [Peer]),
    Mod:link_up(Peer).

-spec link_down(Mod :: module(), To :: #peer{}) -> ok.
%% @doc Notify callback module <em>Mod</em> about a link_down event.
link_down(Mod, To) ->
    lager:info("Link down: ~p~n", [To]), 
    ok = Mod:link_down(To).

-spec start_connection(NodeId :: id(), Opts :: options()) ->
                              {ok, inet:socket()} | {error, any()}.
%% @doc Start a connection to a remote host
start_connection({RemoteIp, RemotePort}, Opts) ->
    {LocalIp, _} = opts:myself(Opts),
<<<<<<< HEAD
    Args = [{reuseaddr, true}, {ip, LocalIp}, {active, false}, binary,
            {send_timeout, opts:send_timeout(Opts)}, {packet, raw},
            {nodelay, true}],
=======
    
    Args = [{reuseaddr, true}, {ip, LocalIp}, {active, false}, binary,
            {send_timeout, opts:send_timeout(Opts)}, {packet, raw},
            {nodelay, true}],
    
>>>>>>> 689e5e0f23ab10567da742a351e192d017919275
    gen_tcp:connect(RemoteIp, RemotePort, Args, opts:timeout(Opts)).

%% @doc Return the first ip address that isn't the loopback interface
get_ip_from_iflist() ->
    {ok, IfList} = inet:getif(),
    L = {127,0,0,1},
    {IP, _, _} = hd(lists:keydelete(L, 1, IfList)),
    IP.

-spec parse_packets(C :: #conn{}, Bin :: binary()) ->
                           {next_state, active, #conn{}} |
                           {stop, normal, #conn{}}.
%% @doc Parse the incoming stream of bytes in the active state and
%%      take appropriate action. If there is residue data save it
%%      until more becomes available.
%%      Only data-messages, forward-joins, shuffles and disconnects
%%      are possible/allowed.

%% Data packet, parse and deliver
parse_packets(C, <<?MESSAGE, Len:32/integer, Rest0/binary>>)
  when byte_size(Rest0) >= Len -> 
    <<Msg:Len/binary, Rest/binary>> = Rest0,
    deliver(C#conn.mod, C#conn.remote, Msg),
    parse_packets(C, Rest);

%% Forward join message, send it to hypar_node
parse_packets(C, <<?FORWARDJOIN, BReq:6/binary, TTL/integer, Rest0/binary>>) ->
    ok = hypar_node:forward_join(C#conn.remote, decode_id(BReq), TTL),
    parse_packets(C, Rest0);

%% Shuffle message, send it to hypar_node
parse_packets(C, <<?SHUFFLE, BReq:6/binary, TTL/integer, Len/integer,
                   Rest0/binary>>) when byte_size(Rest0) >= Len*6 ->
    XListLen = Len*6,
    <<BinXList:XListLen/binary, Rest/binary>> = Rest0,

    ok = hypar_node:shuffle(C#conn.remote, decode_id(BReq),
                            TTL, parse_xlist(BinXList)),
    parse_packets(C, Rest);

%% Disconnect, notify callback and close the socket
parse_packets(C, <<?DISCONNECT>>) ->
    link_down(C#conn.mod, C#conn.remote),
    ok = hypar_node:disconnect(C#conn.remote),
    gen_tcp:close(C#conn.socket),
    {stop, normal, C};

%% No more data to parse, move to active
parse_packets(C, <<>>) ->
    {next_state, active, C};

%% No match, wait until more data is available
parse_packets(C, Rest) ->
    {next_state, active, C#conn{data=Rest}}.

-spec receive_id(C :: #conn{}, Timeout :: timeout()) ->
                        {ok, id()} | {error, any()}.
%% @doc Try to receive an identifier and decoding it. Might return an error.
receive_id(C, Timeout) ->
    case gen_tcp:recv(C#conn.socket, 6, Timeout) of
        {ok, BId} -> {ok, decode_id(BId)};
        Err -> Err
    end.

-spec try_send(C :: #conn{}, Message :: binary() | iolist()) ->
                      {next_state, active, #conn{}} |
                      {stop, {error, any()}, #conn{}}.
%% @doc Try to send <em>Message</em> over active connection <em>C</em>. Move
%%      to active if successful otherwise stop.
try_send(C, Message) ->
    case gen_tcp:send(C#conn.socket, Message) of
        ok ->
            {next_state, active, C};
        {error, Reason} -> 
            ok = hypar_node:error(C#conn.remote, Reason),
            {stop, {error, Reason}, C}
    end.

-spec parse_xlist(Bin :: binary()) -> xlist().
%% @pure
%% @doc Parse an XList
parse_xlist(<<>>) -> [];
parse_xlist(<<Id:6/binary, Rest/binary>>) -> [decode_id(Id)|parse_xlist(Rest)].

-spec encode_xlist(XList :: xlist()) -> binary().
%% @pure
%% @doc Encode an XList
encode_xlist(XList) ->
    encode_xlist(XList, <<>>).

encode_xlist([], All) -> All;
encode_xlist([Id|Rest], All) ->
    BId = encode_id(Id),
    encode_xlist(Rest, <<BId:6/binary, All/binary>>).
                     
-spec encode_id(id()) -> binary().
%% @pure
%% @doc Encode an identifier as a binary
encode_id({{A,B,C,D},Port}) ->
    <<A/integer, B/integer, C/integer, D/integer, Port:16/integer>>.

-spec decode_id(binary()) -> id().
%% @pure
%% @doc Parse a 6-byte identifier
decode_id(<<A/integer, B/integer, C/integer, D/integer, Port:16/integer>>) ->
    {{A, B, C, D}, Port}.

-spec initial_state(Socket :: inet:socket(), Opts :: options()) -> #conn{}.
%% @pure
%% @doc Create the inital state from <em>Opts</em>.
initial_state(Socket, Opts) ->
    #conn{mod=opts:callback(Opts),
          socket = Socket,
          data = <<>>}.

%% @doc Set socket in active mode
socket_active(Socket) -> inet:setopts(Socket, [{active, true}]).

-spec peer(Identifier :: id()) -> #peer{}.
%% @pure
%% @doc Wrapper around a peer-record.
peer(Identifier) ->
    #peer{id=Identifier, conn=self()}.

%% @doc Start ranch without option id defined.
get_ip_and_port(Options) ->
    IP = proplists:get_value(ip, Options, get_ip_from_iflist()),
    Port = proplists:get_value(port, Options, 0),
    {IP, Port}.
