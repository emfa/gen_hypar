%% -------------------------------------------------------------------
%% @author Emil Falk <emil.falk.1988@gmail.com>
%% @copyright (C) 2012, Emil Falk
%% @title Include file
%% @doc Defines records & types used in gen_hypar
%% -------------------------------------------------------------------

-type socket() :: inet:socket().

%% @doc An <em>identifier</em> is a tuple of an IP address and port number
-type id() :: {{byte(), byte(), byte(), byte()},
               inet:port_number()}.

%% Size of an identifier in bytes
-define(IDSIZE, 6).

%% @doc Represent the priority of a neighbour request
-type priority() :: high | low.

%% @doc Represent the options
-type options() :: list(option()).

%% @doc All valid options
-type option() ::
        {active_size,    pos_integer()} | %% Maximum number of active links
        {passive_size,   pos_integer()} | %% Maximum number of passive nodes
        {arwl,           pos_integer()} | %% Active random walk length
        {prwl,           pos_integer()} | %% Passive random walk length
        {k_active,       pos_integer()} | %% k samples from active view in shuffle
        {k_passive,      pos_integer()} | %% k samples from passive view in shuffle
        {shuffle_period, pos_integer()} | %% Shuffle period timer in milliseconds
        {timeout,        timeout()}     | %% Receive timeout, when to consider nodes dead
        {send_timeout,   timeout()}     | %% Send timeout, when to consider nodes dead
        {keep_alive,     pos_integer()}.  %% Time between keep-alive messages

%% @doc Type of peers returned by get_peers and link_up
-type peer() :: {id(), pid()}.

%% @doc Type of an active peer: {Identifier, Pid, MRef}
-type active_peer() :: {id(), pid(), reference()}. 

%% @doc Type of active view
-type active_view() :: list(active_peer()).

%% @doc A list of peer ids
-type view() :: list(id()).

%% @doc Type synonym, view()
-type passive_view() :: view().

%% @doc Exchange lists are only a list of ids
-type xlist() :: view().

%% @doc Internal type for time to live
-type ttl() :: non_neg_integer().

%% @doc Type of the messages that are transfered before a peer becomes active
-type pre_active_message() :: {shuffle_reply, xlist()} |
                              {join, id()} |
                              {join_reply, id()} |
                              {neighbour, id(), priority()}.

%% @doc Type of the messages that are transfered in the active state of a peer
-type active_message() :: {message, binary()} |
                          {forward_join, id(), ttl()} |
                          {shuffle, id(), ttl(), xlist()} |
                          keep_alive |
                          disconnect.
