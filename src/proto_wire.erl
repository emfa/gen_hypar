%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @private
%% @title Implements the on-wire-protocol
%% @doc The implementation of ze bits 'n ze bytes 
%% -------------------------------------------------------------------
-module(proto_wire).

-include("gen_hypar.hrl").

-export([start_connection/3, close/1, send/2, send_join/2, send_join_reply/2,
         send_neighbour_request/3, wait_for_neighbour_reply/2, send_accept/1,
         send_decline/1, send_message/2, send_forward_join/3, send_shuffle/4,
         send_keep_alive/1, send_disconnect/1, send_shuffle_reply/2]).
-export([message/1, forward_join/2, shuffle/3]).
-export([handle_incoming_connection/2, decode_msg/1]).

%% Protocol string, a new connection should always start with this
-define(PROTOSTR, "hypar").
-define(PROTOSTRSIZE, 5).

%% Byte representation of the different messages
-define(MESSAGE,      1). %% Payload message
-define(JOIN,         2). %% Join message
-define(FORWARDJOIN,  3). %% Forward join message
-define(JOINREPLY,    4). %% Forward join reply
-define(HNEIGHBOUR,   5). %% High priority neighbour request
-define(LNEIGHBOUR,   6). %% Low priority neighbour request
-define(SHUFFLE,      7). %% Shuffle request
-define(SHUFFLEREPLY, 8). %% Shuffle reply message
-define(DISCONNECT,   9). %% Disconnect message
-define(KEEPALIVE,   10). %% Keep alive message

%% Either accept or decline
-define(DECLINE, 0). %% Decline a neighbour request
-define(ACCEPT,  1). %% Accept a neighbour request

-spec handle_incoming_connection(socket(), options()) -> pre_active_message().
%% @private These are the possible incoming first messages on a connection
handle_incoming_connection(Socket, Options) ->
    inet:setopts(Socket, sockopts(Options)),
    Timeout = gen_hypar_opts:timeout(Options),
    {ok, <<?PROTOSTR>>} = gen_tcp:recv(Socket, ?PROTOSTRSIZE, Timeout),
    {ok, <<PacketLength:32>>} = gen_tcp:recv(Socket, 4, Timeout),
    {ok, Packet} = gen_tcp:recv(Socket, PacketLength, Timeout),
    case Packet of
        <<?SHUFFLEREPLY, BXList/binary>> ->
            {shuffle_reply, gen_hypar_util:decode_idlist(BXList)};
        <<?JOIN, Id:?IDSIZE/binary>> ->
            {join, gen_hypar_util:decode_id(Id)};
        <<?JOINREPLY, Id:?IDSIZE/binary>> ->
            {join_reply, gen_hypar_util:decode_id(Id)};
        <<?HNEIGHBOUR, Id:?IDSIZE/binary>> ->
            {neighbour, gen_hypar_util:decode_id(Id), high};
        <<?LNEIGHBOUR, Id:?IDSIZE/binary>> ->
            {neighbour, gen_hypar_util:decode_id(Id), low}
    end.

%% Send functions

-spec send(socket(), iolist()) -> ok.
%% @private Send a binary but packet it with a length header first
send(Socket, IOList) ->
    Bin = erlang:iolist_to_binary(IOList),
    Len = byte_size(Bin),    
    gen_tcp:send(Socket, <<Len:32/integer, Bin/binary>>).

-spec send_join(socket(), id()) -> ok.
%% @private Send a join
send_join(Socket, Identifier) ->
    send(Socket, join(Identifier)).

-spec send_join_reply(socket(), id()) -> ok.
%% @private Send a join reply
send_join_reply(Socket, Identifier) ->
    send(Socket, join_reply(Identifier)).

-spec send_neighbour_request(socket(), id(), priority()) -> ok.
%% @private Send a neighbour request
send_neighbour_request(Socket, Myself, Priority) ->
    send(Socket, neighbour(Myself, Priority)).

-spec send_accept(socket()) -> ok.
%% @private Send an accept byte
send_accept(Socket) ->
    send(Socket, <<?ACCEPT>>).

-spec send_decline(socket()) -> ok.
%% @private Send a decline byte
send_decline(Socket) ->
    send(Socket, <<?DECLINE>>).

-spec send_message(socket(), iolist()) -> ok.
send_message(Socket, Msg) ->
    send(Socket, message(Msg)).

-spec send_forward_join(socket(), id(), ttl()) -> ok.
send_forward_join(Socket, Peer, TTL) ->
    send(Socket, forward_join(Peer, TTL)).

-spec send_shuffle(socket(), id(), ttl(), xlist()) -> ok.
send_shuffle(Socket, Peer, TTL, XList) ->
    send(Socket, shuffle(Peer, TTL, XList)).

-spec send_keep_alive(socket()) -> ok.
send_keep_alive(Socket) ->
    send(Socket, <<?KEEPALIVE>>).

-spec send_disconnect(socket()) -> ok.
send_disconnect(Socket) ->
    send(Socket, <<?DISCONNECT>>).

-spec send_shuffle_reply(socket(), xlist()) -> ok.
%% @private Send a shuffle reply
send_shuffle_reply(Socket, XList) ->
    send(Socket, shuffle_reply(XList)).

-spec wait_for_neighbour_reply(socket(), timeout()) -> accept | decline.
%% @private Wait for a neighbour reply, receive the packet header and
%%          the accept/decline byte.
wait_for_neighbour_reply(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 5, Timeout) of
        {ok, <<1:32, ?ACCEPT>>}  -> accept;                           
        {ok, <<1:32, ?DECLINE>>} -> decline
    end.

%% Start and close a connection

-spec start_connection(id(), id(), options()) -> {ok, socket()}.
%% @private Start a new connection, send the protocol string and return the socket.
start_connection({LocalIp, _}, {RemoteIp, RemotePort}, Options) ->
    Timeout = gen_hypar_opts:timeout(Options),
    Opts = [{ip, LocalIp}|sockopts(Options)],
    {ok, Socket} = gen_tcp:connect(RemoteIp, RemotePort, Opts, Timeout),
    ok = gen_tcp:send(Socket, <<?PROTOSTR>>),
    {ok, Socket}.

-spec sockopts(options()) -> list(gen_tcp:option()).
%% @private The socket options used
sockopts(Options) ->
    [binary, {active, false}, {packet, raw}, {nodelay, true},
     {send_timeout, gen_hypar_opts:send_timeout(Options)}].

-spec close(socket()) -> ok.
%% @private Close a socket
close(Socket) ->
    gen_tcp:close(Socket).

-spec message(iolist()) -> iolist().
message(IOList) ->
    [?MESSAGE, IOList].

-spec join(id()) -> iolist().
join(Identifier) ->
    [?JOIN, gen_hypar_util:encode_id(Identifier)].

-spec join_reply(id()) -> iolist().
join_reply(Identifier) ->
    [?JOINREPLY, gen_hypar_util:encode_id(Identifier)].

-spec forward_join(id(), ttl()) -> iolist().
forward_join(Identifier, TTL) ->
    [?FORWARDJOIN, gen_hypar_util:encode_id(Identifier), TTL].

-spec neighbour(id(), priority()) -> iolist().
neighbour(Identifier, high) ->
    [?HNEIGHBOUR, gen_hypar_util:encode_id(Identifier)];
neighbour(Identifier, low) ->
    [?LNEIGHBOUR, gen_hypar_util:encode_id(Identifier)].

-spec shuffle(id(), ttl(), xlist()) -> iolist().
shuffle(Identifier, TTL, XList) ->
    [?SHUFFLE, gen_hypar_util:encode_id(Identifier), TTL,
     gen_hypar_util:encode_idlist(XList)].

-spec shuffle_reply(xlist()) -> iolist().
shuffle_reply(XList) ->
    [?SHUFFLEREPLY, gen_hypar_util:encode_idlist(XList)].

-spec decode_msg(binary()) -> active_message().
%% @doc Decode incoming binaries
decode_msg(<<?MESSAGE, Msg/binary>>) ->
    {message, Msg};
decode_msg(<<?FORWARDJOIN, BId:?IDSIZE/binary, TTL/integer>>) ->
    {forward_join, gen_hypar_util:decode_id(BId), TTL};
decode_msg(<<?SHUFFLE, BId:?IDSIZE/binary, TTL/integer, BXList/binary>>) ->
    {shuffle, gen_hypar_util:decode_id(BId), TTL,
     gen_hypar_util:decode_idlist(BXList)};
decode_msg(<<?DISCONNECT>>) ->
    disconnect;
decode_msg(<<?KEEPALIVE>>) ->
    keep_alive.
