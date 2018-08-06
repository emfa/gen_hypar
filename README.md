# gen_hypar

This is an implementation in Erlang of the [Hybrid Partial View][] membership protocol by João Leitão, José Pereira and Luís Rodrogies.

[Hybrid Partial View]: http://docs.di.fc.ul.pt/jspui/bitstream/10455/2981/1/07-13.pdf

## Quickstart

### Firing up a node
This is a general behaviour for processes that relies on a group membership service. To start a process that runs along side of the group membership serivce we need to define the callback module for  the ``gen_hypar`` behaviour. The callback needs to implement one function:
               
               start_link(Identifier, ModuleArgs) -> {ok, pid()}.

This should spin up a new process. The first thing this process needs to do is call ``gen_hypar:register_and_wait(Identifier)`` to keep things synchronized with all the other processes (see the [supervision tree](#supervision-tree) below). To start the behaviour call:
               
               gen_hypar:start_link(Identifier, Module, ModuleArgs, Options)

This will start up the supervision tree with an isolated node running the HyparView protocol. ``Identifier`` being the ``{Ip, Port}`` tuple that the node is reachable on, ``Module`` and ``ModuleArgs`` are the callback module and arguments. ``Options`` are the gen_hypar-related options. All options are listed at the [bottom](#application-options).

The options should be the same for all nodes in a cluster and otherwise the semantics of the program is not well defined.

When things are fired up we probably want to reach the gen_hypar process, this can be done via:
               
               gen_hypar:where(Identifier) -> undefined | pid().

### Enter the cluster

The node is started isolated and alone until it is joined to the cluster:

               gen_hypar:join_cluster(Identifier, Contact) -> ok | {error, could_not_connect}
               
This joins the node ``Identifier`` to ``Contact``.

If the node recovers from some previous error and start up again on the same ip and port then there is the possibility that it automatically rejoins the cluster. This is because it will with very high probability lie in some nodes passive view. Though it's probably smarter to just rejoin after a disconnect.

### Where are the peers?
As soon as the node has been joined to a cluster the gen_hypar-process will start receiving messages on the form:
               
               {link_up, {PeerId, Pid}}
               {link_down, PeerId}
               {message, PeerId, Message}

These are rather self-explainatory. 
* ``link_up`` - When a new active peer becomes available.
* ``link_down`` - When an active neighbour becomes unavailable or disconnect.
* ``message`` - When a message is received.

If you for some reason want to run only the group-membership service then implement a no-op module and retrive the peers with:
               
               gen_hypar:get_peers(Identifier) -> list({id(), pid()}

### Sending data
To send data you first need to have the pid of an active peer as explained above. Then you use the function:
               
               gen_hypar:send_message(Pid, Message) -> ok.

The `Message` should be an ``iolist()``. Thus a ``binary()`` will also work.

### Example
There is an example module included in examples/flooder.erl. *flooder* is a reliable flooding broadcaster, though very simple and naive. 

## Supervision tree
This is rather implementation specific. The call to ``gen_hypar:start_link/4`` will return the pid of the top supervisor. Thus this call can be used to incorporate the process into another supervision tree. The tree looks like:

                        gen_hypar_sup
                       /      |      \ 
               peer_sup   hypar_node  gen_hypar
                  |
               peers

The peers themselves are a supervision tree of 4 processes:

                                 peer
                              /    |    \
                             /     |     \
                      peer_ctl peer_send peer_recv

``peer_ctl`` is the middle-man between the ``hypar_node`` which is responsible for the protocol logic and the gen_hypar-process. ``peer_send`` and ``peer_recv`` are responsible for on-wire sending and receiving.

## Other projects on top of hyparerl
Check out [plumcast][], it's in early development. It is an implementation of the Plumtree protocol developed by the same guys. Plumcast builds a broadcast tree on top of hyparerl to reduce the network traffic without sacrificing to much latency.

Also check out [floodcast][], also very early development, that will basically be a more serious implementation of the simple flooder example.

[plumcast]: emfa/plumcast
[floodcast]: emfa/floodcast

## Application options

<table>
  <tr>
    <td> <b>active_size</b>    </td>
    <td> Maximum entries in the active view.</td>
    <td>5</td>
  </tr>
  <tr>
    <td> <b>passive_size</b>   </td>
    <td> Same as above but for passive view.</td>
    <td>30</td>
  </tr>
  <tr>
    <td> <b>arwl</b>           </td>
    <td> Active Random Walk Length.</td>
    <td>6</td>
  </tr>
  <tr>
    <td> <b>prwl</b>           </td>
    <td> Passive Random Walk Length.</td>
    <td>3</td>
  </tr>
  <tr>
    <td> <b>k_active</b>       </td>
    <td> The number of nodes sampled from the active view when doing a shuffle.</td>
    <td>3</td>
  </tr>
  <tr>
    <td> <b>k_passive</b>      </td>
    <td> Same as above but with passive view.</td>
    <td>4</td>
  </tr>
  <tr>
    <td> <b>shuffle_period</b> </td>
    <td> Cyclic period timer of when to do a shuffle in milliseconds.</td>
    <td>10000</td>
  </tr>
  <tr>
    <td> <b>keep_alive</b>     </td>
    <td> The time between heartbeat messages. Fine tune the responsiveness to failures.</td>
    <td>NOTIMPL</td>
  </tr>
</table>
