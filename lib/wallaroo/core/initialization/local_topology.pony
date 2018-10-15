/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

use "buffered"
use "collections"
use "files"
use "net"
use "promises"
use "serialise"
use "wallaroo"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/keys"
use "wallaroo/ent/barrier"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/cluster_manager"
use "wallaroo/ent/network"
use "wallaroo/ent/recovery"
use "wallaroo/ent/router_registry"
use "wallaroo/ent/checkpoint"
use "wallaroo/core/data_channel"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/routing"
use "wallaroo/core/sink/tcp_sink"
use "wallaroo/core/source"
use "wallaroo/core/source/barrier_source"
use "wallaroo/core/topology"
use "wallaroo_labs/collection_helpers"
use "wallaroo_labs/dag"
use "wallaroo_labs/equality"
use "wallaroo_labs/messages"
use "wallaroo_labs/mort"
use "wallaroo_labs/queue"

class val LocalTopology
  let _app_name: String
  let _worker_name: WorkerName

  let _graph: Dag[StepInitializer] val

  // A map from node ids in the graph to one or more routing ids per node,
  // depending on the parallelism.
  let _routing_ids: Map[U128, SetIs[RoutingId] val] val

  //!@
  // let _step_map: Map[RoutingId, (ProxyAddress | RoutingId)] val
  // // _state_builders maps from state_name to StateSubpartitions
  // let _state_builders: Map[StateName, StateSubpartitions] val
  // let _pre_state_data: Array[PreStateData] val
  // let _boundary_ids: Map[WorkerName, RoutingId] val
  // let state_step_ids: Map[StateName, Array[RoutingId] val] val

  let state_routing_ids: Map[StateName, Map[WorkerName, RoutingId] val] val
  let stateless_partition_routing_ids:
    Map[RoutingId, Map[WorkerName, RoutingId] val] val
  let worker_names: Array[WorkerName] val
  // Workers that cannot be removed during shrink to fit
  let non_shrinkable: SetIs[WorkerName] val
  // Each worker has one BarrierSource with a unique id
  let barrier_source_id: RoutingId

  new val create(name': String, worker_name': WorkerName,
    graph': Dag[StepInitializer] val,
    routing_ids': Map[U128, SetIs[RoutingId] val] val,
    //!@
    // step_map': Map[RoutingId, (ProxyAddress | RoutingId)] val,
    // state_builders': Map[StateName, StateSubpartitions] val,
    // pre_state_data': Array[PreStateData] val,
    // boundary_ids': Map[WorkerName, RoutingId] val,
    // state_step_ids': Map[StateName, Array[RoutingId] val] val,
    worker_names': Array[WorkerName] val,
    non_shrinkable': SetIs[WorkerName] val,
    state_routing_ids': Map[StateName, Map[WorkerName, RoutingId] val] val,
    stateless_partition_routing_ids':
      Map[RoutingId, Map[WorkerName, RoutingId] val] val,
    barrier_source_id': RoutingId)
  =>
    _app_name = name'
    _worker_name = worker_name'
    _graph = graph'
    _routing_ids = routing_ids'
    //!@
    // _step_map = step_map'
    // _state_builders = state_builders'
    // _pre_state_data = pre_state_data'
    // _boundary_ids = boundary_ids'
    // state_step_ids = state_step_ids'
    worker_names = worker_names'
    non_shrinkable = non_shrinkable'
    state_routing_ids = state_routing_ids'
    stateless_partition_routing_ids = stateless_partition_routing_ids'
    barrier_source_id = barrier_source_id'

  //!@
  // fun state_builders(): Map[StateName, StateSubpartitions] val =>
  //   _state_builders

  fun update_state_map(state_name: StateName,
    state_map: Map[StateName, Router],
    metrics_conn: MetricsSink, event_log: EventLog,
    all_local_keys: Map[StateName, SetIs[Key] val] val,
    recovery_replayer: RecoveryReconnecter,
    auth: AmbientAuth,
    outgoing_boundaries: Map[WorkerName, OutgoingBoundary] val,
    initializables: Initializables,
    data_routes: Map[RoutingId, Consumer tag],
    state_steps: Map[StateName, Array[Step] val],
    built_state_step_ids: Map[StateName, Map[RoutingId, Step] val],
    router_registry: RouterRegistry)
  =>
    None
    //!@ Rework this logic

    // let subpartition =
    //   try
    //     _state_builders(state_name)?
    //   else
    //     @printf[I32](("Tried to update state map with nonexistent state " +
    //       "name " + state_name + "\n").cstring())
    //     error
    //   end

    // if not state_map.contains(state_name) then
    //   @printf[I32](("----Creating state steps for " + state_name + "----\n")
    //     .cstring())
    //   try
    //     let local_state_routing_ids = state_routing_ids(state_name)?
    //     state_map(state_name) = subpartition.build(_app_name, _worker_name,
    //       worker_names, metrics_conn, auth, event_log, all_local_keys,
    //       recovery_replayer, outgoing_boundaries, initializables, data_routes,
    //       state_step_ids, state_steps, built_state_step_ids,
    //       state_routing_ids(state_name)?, router_registry)
    //   else
    //     Fail()
    //   end
    // end

  fun graph(): Dag[StepInitializer] val => _graph

  //!@
  // fun pre_state_data(): Array[PreStateData] val => _pre_state_data

  // fun step_map(): Map[RoutingId, (ProxyAddress | RoutingId)] val => _step_map

  fun name(): String => _app_name

  fun worker_name(): WorkerName => _worker_name

  fun is_empty(): Bool =>
    _graph.is_empty()

  //!@
  // fun boundary_ids(): Map[WorkerName, RoutingId] val => _boundary_ids

  fun val add_worker_name(w: WorkerName): LocalTopology =>
    if not worker_names.contains(w) then
      let new_worker_names = recover trn Array[WorkerName] end
      for n in worker_names.values() do
        new_worker_names.push(n)
      end
      new_worker_names.push(w)

      let new_state_builders =
        recover iso Map[StateName, StateSubpartitions] end

      //!@
      // for (state_name, state_subpartitions) in _state_builders.pairs() do
      //   new_state_builders(state_name) = state_subpartitions.add_worker_name(w)
      // end

      LocalTopology(_app_name, _worker_name, _graph, _routing_ids,
        consume new_worker_names, non_shrinkable,
        state_routing_ids, stateless_partition_routing_ids, barrier_source_id)
    else
      this
    end

  fun val remove_worker_names(ws: Array[WorkerName] val,
    barrier_initiator: BarrierInitiator): LocalTopology
  =>
    let new_worker_names = recover trn Array[WorkerName] end
    for w in worker_names.values() do
      if not ArrayHelpers[WorkerName].contains[WorkerName](ws, w) then
        new_worker_names.push(w)
      end
    end
    LocalTopology(_app_name, _worker_name, _graph, _routing_ids,
      consume new_worker_names, non_shrinkable,
      state_routing_ids, stateless_partition_routing_ids, barrier_source_id)

  fun val assign_routing_ids(gen: RoutingIdGenerator): LocalTopology =>
    let r_ids = recover iso Map[U128, SetIs[RoutingId] val] end
    for n in _graph.nodes() do
      let next = recover iso SetIs[RoutingId] end
      for _ in Range(0, n.value.parallelism()) do
        next.set(gen())
      end
      r_ids(n.id) = consume next
    end
    LocalTopology(_app_name, _worker_name, _graph, consume r_ids,
      worker_names, non_shrinkable,
      state_routing_ids, stateless_partition_routing_ids, barrier_source_id)

  fun val add_state_routing_ids(worker: WorkerName,
    sri: Map[StateName, RoutingId] val): LocalTopology
  =>
    let new_state_routing_ids =
      recover iso Map[StateName, Map[WorkerName, RoutingId] val] end
    for (state_name, workers) in state_routing_ids.pairs() do
      let w_ids = recover iso Map[WorkerName, RoutingId] end
      for (w, id) in workers.pairs() do
        w_ids(w) = id
      end
      try
        w_ids(worker) = sri(state_name)?
      else
        Fail()
      end
      new_state_routing_ids(state_name) = consume w_ids
    end
    LocalTopology(_app_name, _worker_name, _graph, _routing_ids,
      worker_names, non_shrinkable,
      consume new_state_routing_ids, stateless_partition_routing_ids,
      barrier_source_id)

  fun val add_stateless_partition_routing_ids(worker: WorkerName,
    spri: Map[RoutingId, RoutingId] val): LocalTopology
  =>
    let new_stateless_partition_routing_ids =
      recover iso Map[RoutingId, Map[WorkerName, RoutingId] val] end
    for (p_id, workers) in stateless_partition_routing_ids.pairs() do
      let w_ids = recover iso Map[WorkerName, RoutingId] end
      for (w, id) in workers.pairs() do
        w_ids(w) = id
      end
      try
        w_ids(worker) = spri(p_id)?
      else
        Fail()
      end
      new_stateless_partition_routing_ids(p_id) = consume w_ids
    end
    LocalTopology(_app_name, _worker_name, _graph, _routing_ids,
      worker_names, non_shrinkable, state_routing_ids,
      consume new_stateless_partition_routing_ids, barrier_source_id)

  //!@
  fun val for_new_worker(new_worker: WorkerName): LocalTopology =>
    this

    //!@ Rework this logic

    // let w_names =
    //   if not worker_names.contains(new_worker) then
    //     add_worker_name(new_worker).worker_names
    //   else
    //     worker_names
    //   end

    // let g = Dag[StepInitializer]
    // // Pick up sinks, which are shared across workers
    // for node in _graph.nodes() do
    //   match node.value
    //   | let egress: EgressBuilder =>
    //     g.add_node(egress, node.id)
    //   end
    // end

    // // Assign state step ids
    // let worker_state_step_ids =
    //   recover iso Map[StateName, Array[RoutingId] val] end
    // let routing_id_gen = RoutingIdGenerator
    // for state_name in state_routing_ids.keys() do
    //   let next_state_step_ids = recover iso Array[RoutingId] end
    //   let parallelism =
    //     state_builders()(state_name)?.per_worker_parallelism()
    //   for _ in Range(0, parallelism) do
    //     next_state_step_ids.push(routing_id_gen())
    //   end
    //   worker_state_step_ids(state_name) = consume next_state_step_ids
    // end

    // let lt = LocalTopology(_app_name, new_worker, g.clone()?, _routing_ids,
    //   consume worker_state_step_ids, w_names, non_shrinkable,
    //   state_routing_ids, stateless_partition_routing_ids, barrier_source_id)

    // lt.assign_routing_ids()

  fun eq(that: box->LocalTopology): Bool =>
    // This assumes that _graph and _pre_state_data never change over time
    (_app_name == that._app_name) and
      (_worker_name == that._worker_name) and
      (_graph is that._graph) and
      //!@
      // MapEquality2[U128, ProxyAddress, U128](_step_map, that._step_map)
      //   and
      // MapEquality[String, StateSubpartitions](_state_builders,
      //   that._state_builders) and
      // (_pre_state_data is that._pre_state_data) and
      // MapEquality[String, U128](_boundary_ids, that._boundary_ids) and
      ArrayEquality[String](worker_names, that.worker_names)

  fun ne(that: box->LocalTopology): Bool => not eq(that)

actor LocalTopologyInitializer is LayoutInitializer
  let _app_name: String
  let _worker_name: WorkerName
  let _env: Env
  let _auth: AmbientAuth
  let _connections: Connections
  let _router_registry: RouterRegistry
  let _metrics_conn: MetricsSink
  let _data_receivers: DataReceivers
  let _event_log: EventLog
  let _recovery: Recovery
  let _recovery_replayer: RecoveryReconnecter
  let _checkpoint_initiator: CheckpointInitiator
  let _barrier_initiator: BarrierInitiator
  var _is_initializer: Bool
  var _outgoing_boundary_builders:
    Map[WorkerName, OutgoingBoundaryBuilder] val =
      recover Map[WorkerName, OutgoingBoundaryBuilder] end
  var _outgoing_boundaries: Map[WorkerName, OutgoingBoundary] val =
    recover Map[WorkerName, OutgoingBoundary] end
  var _topology: (LocalTopology | None) = None
  let _local_topology_file: String
  var _cluster_initializer: (ClusterInitializer | None) = None
  let _data_channel_file: String
  let _worker_names_file: String
  let _local_keys_file: LocalKeysFile
  let _the_journal: SimpleJournal
  let _do_local_file_io: Bool
  var _topology_initialized: Bool = false
  var _recovered_worker_names: Array[WorkerName] val =
    recover val Array[WorkerName] end
  var _recovering: Bool = false
  let _is_joining: Bool
  // If we're joining, we need to generate our own state routing ids
  let _joining_state_routing_ids: (Map[StateName, RoutingId] val | None)
  let _joining_stateless_partition_routing_ids:
    (Map[RoutingId, RoutingId] val | None)

  let _routing_id_gen: RoutingIdGenerator = RoutingIdGenerator

  // Lifecycle
  var _created: SetIs[Initializable] = _created.create()
  var _initialized: SetIs[Initializable] = _initialized.create()
  var _ready_to_work: SetIs[Initializable] = _ready_to_work.create()
  let _initializables: Initializables = Initializables
  var _event_log_ready_to_work: Bool = false
  var _recovery_ready_to_work: Bool = false
  var _initialization_lifecycle_complete: Bool = false


  // Partition router blueprints
  var _partition_router_blueprints:
    Map[StateName, PartitionRouterBlueprint] val =
      recover Map[StateName, PartitionRouterBlueprint] end
  var _stateless_partition_router_blueprints:
    Map[U128, StatelessPartitionRouterBlueprint] val =
      recover Map[U128, StatelessPartitionRouterBlueprint] end

  // Accumulate all TCPSourceListenerBuilders so we can build them
  // once EventLog signals we're ready
  let sl_builders: Array[SourceListenerBuilder] =
    recover iso Array[SourceListenerBuilder] end

  // Cluster Management
  var _cluster_manager: (ClusterManager | None) = None

  var _t: USize = 0

  new create(app_name: String, worker_name: WorkerName, env: Env,
    auth: AmbientAuth, connections: Connections,
    router_registry: RouterRegistry, metrics_conn: MetricsSink,
    is_initializer: Bool, data_receivers: DataReceivers,
    event_log: EventLog, recovery: Recovery,
    recovery_replayer: RecoveryReconnecter,
    checkpoint_initiator: CheckpointInitiator, barrier_initiator: BarrierInitiator,
    local_topology_file: String, data_channel_file: String,
    worker_names_file: String, local_keys_filepath: FilePath,
    the_journal: SimpleJournal, do_local_file_io: Bool,
    cluster_manager: (ClusterManager | None) = None,
    is_joining: Bool = false,
    joining_state_routing_ids: (Map[StateName, RoutingId] val | None) = None,
    joining_stateless_partition_routing_ids:
      (Map[RoutingId, RoutingId] val | None) = None)
  =>
    _app_name = app_name
    _worker_name = worker_name
    _env = env
    _auth = auth
    _connections = connections
    _router_registry = router_registry
    _metrics_conn = metrics_conn
    _is_initializer = is_initializer
    _data_receivers = data_receivers
    _event_log = event_log
    _recovery = recovery
    _recovery_replayer = recovery_replayer
    _checkpoint_initiator = checkpoint_initiator
    _barrier_initiator = barrier_initiator
    _local_topology_file = local_topology_file
    _data_channel_file = data_channel_file
    _worker_names_file = worker_names_file
    _local_keys_file = LocalKeysFile(local_keys_filepath, the_journal, auth,
      do_local_file_io)
    _the_journal = the_journal
    _do_local_file_io = do_local_file_io
    _cluster_manager = cluster_manager
    _is_joining = is_joining
    _joining_state_routing_ids = joining_state_routing_ids
    _joining_stateless_partition_routing_ids =
      joining_stateless_partition_routing_ids
    _router_registry.register_local_topology_initializer(this)
    _initializables.set(_checkpoint_initiator)
    _initializables.set(_barrier_initiator)
    _recovery.update_initializer(this)

  be update_topology(t: LocalTopology) =>
    _topology = t

  be connect_to_joining_workers(coordinator: String,
    control_addrs: Map[WorkerName, (String, String)] val,
    data_addrs: Map[WorkerName, (String, String)] val,
    new_state_routing_ids: Map[WorkerName, Map[StateName, RoutingId] val] val,
    new_stateless_partition_routing_ids:
      Map[WorkerName, Map[RoutingId, RoutingId] val] val)
  =>
    let new_workers = recover iso Array[WorkerName] end
    for w in control_addrs.keys() do new_workers.push(w) end
    _router_registry.connect_to_joining_workers(consume new_workers,
      new_state_routing_ids, new_stateless_partition_routing_ids, coordinator)

    for w in control_addrs.keys() do
      try
        let host = control_addrs(w)?._1
        let control_addr = control_addrs(w)?
        let data_addr = data_addrs(w)?
        let state_routing_ids = new_state_routing_ids(w)?
        let stateless_partition_routing_ids =
          new_stateless_partition_routing_ids(w)?
        _add_joining_worker(w, host, control_addr, data_addr,
          state_routing_ids, stateless_partition_routing_ids)
      else
        Fail()
      end
    end
    _save_local_topology()

  be add_joining_worker(w: WorkerName, joining_host: String,
    control_addr: (String, String), data_addr: (String, String),
    state_routing_ids: Map[StateName, RoutingId] val,
    stateless_partition_routing_ids: Map[RoutingId, RoutingId] val)
  =>
    _add_joining_worker(w, joining_host, control_addr, data_addr,
      state_routing_ids, stateless_partition_routing_ids)
    _save_local_topology()

  fun ref _add_joining_worker(w: WorkerName, joining_host: String,
    control_addr: (String, String), data_addr: (String, String),
    state_routing_ids: Map[StateName, RoutingId] val,
    stateless_partition_routing_ids: Map[RoutingId, RoutingId] val)
  =>
    match _topology
    | let t: LocalTopology =>
      if not ArrayHelpers[String].contains[String](t.worker_names, w) then
        let updated_topology = _add_worker_name(w, t)
        _connections.create_control_connection(w, joining_host,
          control_addr._2)
        let new_boundary_id = _routing_id_gen()
        _connections.create_data_connection_to_joining_worker(w,
          joining_host, data_addr._2, new_boundary_id, state_routing_ids,
          stateless_partition_routing_ids, this)
        _connections.save_connections()
        _topology = updated_topology
          .add_state_routing_ids(w, state_routing_ids)
          .add_stateless_partition_routing_ids(w,
            stateless_partition_routing_ids)
        @printf[I32]("***New worker %s added to cluster!***\n".cstring(),
          w.cstring())
      end
    else
      Fail()
    end

  be initiate_shrink(target_workers: Array[WorkerName] val, shrink_count: U64,
    conn: TCPConnection)
  =>
    if target_workers.size() > 0 then
      if _are_valid_shrink_candidates(target_workers) then
        let remaining_workers = _remove_worker_names(target_workers)
        _router_registry.inject_shrink_autoscale_barrier(remaining_workers,
          target_workers)
        let reply = ExternalMsgEncoder.shrink_error_response(
          "Shrinking by " + target_workers.size().string() + " workers!")
        conn.writev(reply)
      else
        @printf[I32]("**Invalid shrink targets!**\n".cstring())
        let error_reply = ExternalMsgEncoder.shrink_error_response(
          "Invalid shrink targets!")
        conn.writev(error_reply)
      end
    elseif shrink_count > 0 then
      let candidates = _get_shrink_candidates(shrink_count.usize())
      if candidates.size() < shrink_count.usize() then
        @printf[I32]("**Only %s candidates are eligible for removal\n"
          .cstring(), candidates.size().string().cstring())
      else
        @printf[I32]("**%s candidates are eligible for removal\n"
          .cstring(), candidates.size().string().cstring())
      end
      if candidates.size() > 0 then
        let remaining_workers = _remove_worker_names(candidates)
        _router_registry.inject_shrink_autoscale_barrier(remaining_workers,
          candidates)
        let reply = ExternalMsgEncoder.shrink_error_response(
          "Shrinking by " + candidates.size().string() + " workers!")
        conn.writev(reply)
      else
        @printf[I32]("**Cannot shrink 0 workers!**\n".cstring())
        let error_reply = ExternalMsgEncoder.shrink_error_response(
          "Cannot shrink 0 workers!")
        conn.writev(error_reply)
      end
    else
      @printf[I32]("**Cannot shrink 0 workers!**\n".cstring())
      let error_reply = ExternalMsgEncoder.shrink_error_response(
        "Cannot shrink 0 workers!")
      conn.writev(error_reply)
    end

  be take_over_initiate_shrink(remaining_workers: Array[WorkerName] val,
    leaving_workers: Array[WorkerName] val)
  =>
    _remove_worker_names(leaving_workers)
    _router_registry.inject_shrink_autoscale_barrier(remaining_workers,
      leaving_workers)

  be prepare_shrink(remaining_workers: Array[WorkerName] val,
    leaving_workers: Array[WorkerName] val)
  =>
    _remove_worker_names(leaving_workers)
    _router_registry.prepare_shrink(remaining_workers, leaving_workers)

  be remove_worker_connection_info(worker: WorkerName) =>
    _connections.remove_worker_connection_info(worker)
    _connections.save_connections()

  fun _are_valid_shrink_candidates(candidates: Array[WorkerName] val): Bool =>
    match _topology
    | let t: LocalTopology =>
      for c in candidates.values() do
        // A worker name is not a valid candidate if it is non shrinkable
        // or if it's not in the current cluster.
        if SetHelpers[String].contains[String](t.non_shrinkable, c) or
          (not ArrayHelpers[String].contains[String](t.worker_names, c))
        then
          return false
        end
      end
      true
    else
      Fail()
      false
    end

  fun _get_shrink_candidates(count: USize): Array[WorkerName] val =>
    let candidates = recover trn Array[String] end
    match _topology
    | let t: LocalTopology =>
      for w in t.worker_names.values() do
        if candidates.size() < count then
          if not SetHelpers[String].contains[String](t.non_shrinkable, w) then
            candidates.push(w)
          end
        end
      end
    else
      Fail()
    end
    consume candidates

  be add_boundary_to_joining_worker(w: WorkerName, boundary: OutgoingBoundary,
    builder: OutgoingBoundaryBuilder,
    state_routing_ids: Map[StateName, RoutingId] val,
    stateless_partition_routing_ids: Map[RoutingId, RoutingId] val)
  =>
    _add_boundary(w, boundary, builder)
    _router_registry.register_boundaries(_outgoing_boundaries,
      _outgoing_boundary_builders)
    _router_registry.joining_worker_initialized(w, state_routing_ids,
      stateless_partition_routing_ids)

  fun ref _add_worker_name(w: WorkerName, t: LocalTopology): LocalTopology =>
    let updated_topology = t.add_worker_name(w)
    _topology = updated_topology
    _save_local_topology()
    _save_worker_names()
    updated_topology

  fun ref _remove_worker_names(ws: Array[WorkerName] val):
    Array[WorkerName] val
  =>
    match _topology
    | let t: LocalTopology =>
      let new_topology = t.remove_worker_names(ws, _barrier_initiator)
      _topology = new_topology
      _save_local_topology()
      _save_worker_names()
      new_topology.worker_names
    else
      Fail()
      recover val Array[WorkerName] end
    end

  fun ref _add_boundary(target_worker: WorkerName, boundary: OutgoingBoundary,
    builder: OutgoingBoundaryBuilder)
  =>
    // Boundaries
    let bs = recover trn Map[WorkerName, OutgoingBoundary] end
    for (w, b) in _outgoing_boundaries.pairs() do
      bs(w) = b
    end
    bs(target_worker) = boundary

    // Boundary builders
    let bbs = recover trn Map[WorkerName, OutgoingBoundaryBuilder] end
    for (w, b) in _outgoing_boundary_builders.pairs() do
      bbs(w) = b
    end
    bbs(target_worker) = builder

    _outgoing_boundaries = consume bs
    _outgoing_boundary_builders = consume bbs
    _initializables.set(boundary)

  be remove_boundary(leaving_worker: WorkerName) =>
    // Boundaries
    let bs = recover trn Map[WorkerName, OutgoingBoundary] end
    for (w, b) in _outgoing_boundaries.pairs() do
      if w != leaving_worker then bs(w) = b end
    end

    // Boundary builders
    let bbs = recover trn Map[WorkerName, OutgoingBoundaryBuilder] end
    for (w, b) in _outgoing_boundary_builders.pairs() do
      if w != leaving_worker then bbs(w) = b end
    end
    _outgoing_boundaries = consume bs
    _outgoing_boundary_builders = consume bbs

  be update_boundaries(bs: Map[WorkerName, OutgoingBoundary] val,
    bbs: Map[WorkerName, OutgoingBoundaryBuilder] val)
  =>
    // This should only be called during initialization
    if (_outgoing_boundaries.size() > 0) or
       (_outgoing_boundary_builders.size() > 0)
    then
      Fail()
    end

    _outgoing_boundaries = bs
    _outgoing_boundary_builders = bbs
    // TODO: This no longer captures all boundaries because of boundary per
    // source. Does this matter without backpressure?
    for boundary in bs.values() do
      _initializables.set(boundary)
    end

  be create_data_channel_listener(ws: Array[WorkerName] val,
    host: String, service: String,
    cluster_initializer: (ClusterInitializer | None) = None)
  =>
    try
      let data_channel_filepath = FilePath(_auth, _data_channel_file)?
      if not _is_initializer then
        let data_notifier: DataChannelListenNotify iso =
          DataChannelListenNotifier(_worker_name, _auth, _connections,
            _is_initializer,
            MetricsReporter(_app_name, _worker_name,
              _metrics_conn),
            data_channel_filepath, this, _data_receivers, _recovery_replayer,
            _router_registry, _the_journal, _do_local_file_io)

        _connections.make_and_register_recoverable_data_channel_listener(
          _auth, consume data_notifier, _router_registry,
          data_channel_filepath, host, service)
      else
        match cluster_initializer
          | let ci: ClusterInitializer =>
            _connections.create_initializer_data_channel_listener(
              _data_receivers, _recovery_replayer, _router_registry, ci,
              data_channel_filepath, this)
        end
      end
    else
      @printf[I32]("FAIL: cannot create data channel\n".cstring())
    end

  be create_connections(control_addrs: Map[WorkerName, (String, String)] val,
    data_addrs: Map[WorkerName, (String, String)] val)
  =>
    _connections.create_connections(control_addrs, data_addrs, this,
      _router_registry)

  be quick_initialize_data_connections() =>
    """
    Called as part of joining worker's initialization
    """
    _connections.quick_initialize_data_connections(this)

  be set_partition_router_blueprints(
    pr_blueprints: Map[StateName, PartitionRouterBlueprint] val,
    spr_blueprints: Map[U128, StatelessPartitionRouterBlueprint] val)
  =>
    _partition_router_blueprints = pr_blueprints
    _stateless_partition_router_blueprints = spr_blueprints

  be rollback_local_keys(checkpoint_id: CheckpointId,
    promise: Promise[None])
  =>
    @printf[I32]("Rolling back topology graph.\n".cstring())
    let local_keys = _local_keys_file.read_local_keys(checkpoint_id)
    _router_registry.rollback_keys(local_keys, promise)

  be recover_and_initialize(ws: Array[WorkerName] val,
    target_checkpoint_id: CheckpointId,
    cluster_initializer: (ClusterInitializer | None) = None)
  =>
    _recovering = true
    _recovered_worker_names = ws

    try
      let data_channel_filepath = FilePath(_auth, _data_channel_file)?
      if not _is_initializer then
        let data_notifier: DataChannelListenNotify iso =
          DataChannelListenNotifier(_worker_name, _auth, _connections,
            _is_initializer,
            MetricsReporter(_app_name, _worker_name,
              _metrics_conn),
            data_channel_filepath, this, _data_receivers, _recovery_replayer,
            _router_registry, _the_journal, _do_local_file_io)

        _connections.make_and_register_recoverable_data_channel_listener(
          _auth, consume data_notifier, _router_registry,
          data_channel_filepath)
      else
        match cluster_initializer
        | let ci: ClusterInitializer =>
          _connections.create_initializer_data_channel_listener(
            _data_receivers, _recovery_replayer, _router_registry, ci,
            data_channel_filepath, this)
        end
      end
    else
      @printf[I32]("FAIL: cannot create data channel\n".cstring())
    end

  be register_key(state_name: StateName, key: Key,
    checkpoint_id: (CheckpointId | None) = None)
  =>
    // We only add an entry to the local keys file if this is part of a
    // checkpoint.
    match checkpoint_id
    | let c_id: CheckpointId =>
      _local_keys_file.add_key(state_name, key, c_id)
    end

  be unregister_key(state_name: StateName, key: Key,
    checkpoint_id: (CheckpointId | None) = None)
  =>
    // We only add an entry to the local keys file if this is part of a
    // checkpoint.
    match checkpoint_id
    | let c_id: CheckpointId =>
      _local_keys_file.remove_key(state_name, key, c_id)
    end

  fun ref _save_worker_names()
  =>
    """
    Save the list of worker names to a file.
    """
    try
      match _topology
      | let t: LocalTopology =>
        @printf[I32](("Saving worker names to file: " + _worker_names_file +
          "\n").cstring())
        let worker_names_filepath = FilePath(_auth, _worker_names_file)?
        let file = AsyncJournalledFile(worker_names_filepath, _the_journal, _auth, _do_local_file_io)
        // Clear file
        file.set_length(0)
        for worker_name in t.worker_names.values() do
          file.print(worker_name)
          @printf[I32](("LocalTopology._save_worker_names: " + worker_name +
          "\n").cstring())
        end
        file.sync()
        file.dispose()
        // TODO: AsyncJournalledFile does not provide implicit sync semantics here
      else
        Fail()
      end
    else
      Fail()
    end

  fun ref _save_local_topology() =>
    match _topology
    | let t: LocalTopology =>
      @printf[I32]("Saving topology!\n".cstring())
      try
        let local_topology_file = try
          FilePath(_auth, _local_topology_file)?
        else
          @printf[I32]("Error opening topology file!\n".cstring())
          Fail()
          error
        end
        // TODO: Back up old file before clearing it?
        let file = AsyncJournalledFile(local_topology_file, _the_journal,
          _auth, _do_local_file_io)
        // Clear contents of file.
        file.set_length(0)
        let wb = Writer
        let sa = SerialiseAuth(_auth)
        let s = try
          Serialised(sa, t)?
        else
          @printf[I32]("Error serializing topology!\n".cstring())
          Fail()
          error
        end
        let osa = OutputSerialisedAuth(_auth)
        let serialised_topology: Array[U8] val = s.output(osa)
        wb.write(serialised_topology)
        file.writev(recover val wb.done() end)
        file.sync()
        file.dispose()
        // TODO: AsyncJournalledFile does not provide implicit sync semantics here
      else
        @printf[I32]("Error saving topology!\n".cstring())
        Fail()
      end
    else
      @printf[I32]("Error saving topology!\n".cstring())
      Fail()
    end

  be initialize(cluster_initializer: (ClusterInitializer | None) = None,
    checkpoint_target: (CheckpointId | None) = None)
  =>
    @printf[I32]("!@ STARTING WORKER BUT INITIALIZE IS COMMENTED OUT\n".cstring())

    //!@ TODO: Uncomment all this stuff below!

    // _recovering =
    //   match checkpoint_target
    //   | let id: CheckpointId =>
    //     _recovery.update_checkpoint_id(id)
    //     true
    //   else
    //     false
    //   end

    // if _topology_initialized then
    //   ifdef debug then
    //     // Currently, recovery in a single worker cluster is a special case.
    //     // We do not need to recover connections to other workers, so we
    //     // initialize immediately in Startup. However, we eventually trigger
    //     // code in connections.pony where initialize() is called again. For
    //     // now, this code simply returns in that scenario to avoid double
    //     // initialization.
    //     Invariant(
    //       try (_topology as LocalTopology).worker_names.size() == 1
    //       else false end
    //     )
    //   end
    //   return
    // end

    // if _is_joining then
    //   _initialize_joining_worker()
    //   return
    // end

    // @printf[I32](("------------------------------------------------------" +
    //   "---\n").cstring())
    // @printf[I32]("|v|v|v|Initializing Local Topology|v|v|v|\n\n".cstring())
    // _cluster_initializer = cluster_initializer
    // try
    //   try
    //     let local_topology_file = FilePath(_auth, _local_topology_file)?
    //     if local_topology_file.exists() then
    //       //we are recovering an existing worker topology
    //       let data = recover val
    //         // TODO: We assume that all journal data is copied to local file system first
    //         let file = File(local_topology_file)
    //         file.read(file.size())
    //       end
    //       match Serialised.input(InputSerialisedAuth(_auth), data)(
    //         DeserialiseAuth(_auth))?
    //       | let t: LocalTopology val =>
    //         _topology = t
    //       else
    //         @printf[I32]("Error restoring previous topology!".cstring())
    //         Fail()
    //       end
    //     end
    //   else
    //     @printf[I32]("Error restoring previous topology!".cstring())
    //     Fail()
    //   end

    //   match _topology
    //   | let t: LocalTopology =>
    //     let worker_count = t.worker_names.size()

    //     if (worker_count > 1) and (_outgoing_boundaries.size() == 0) then
    //       @printf[I32]("Outgoing boundaries not set up!\n".cstring())
    //       error
    //     end

    //     for w in t.worker_names.values() do
    //       _barrier_initiator.add_worker(w)
    //       _checkpoint_initiator.add_worker(w)
    //     end

    //     _save_local_topology()
    //     _save_worker_names()

    //     // Determine local keys for all state collections
    //     let local_keys: Map[StateName, SetIs[Key] val] val =
    //       if not _recovering then
    //         let lks = recover iso Map[StateName, SetIs[Key] val] end
    //         for (s_name, subp) in t.state_builders().pairs() do
    //           lks(s_name) = subp.initial_local_keys(_worker_name)
    //         end
    //         let lks_val = consume val lks

    //         // Populate local keys file
    //         for (s_name, keys) in lks_val.pairs() do
    //           for k in keys.values() do
    //             _local_keys_file.add_key(s_name, k, 1)
    //           end
    //         end

    //         lks_val
    //       else
    //         @printf[I32]("Reading local keys from file.\n".cstring())
    //         try
    //           _local_keys_file.read_local_keys(
    //             checkpoint_target as CheckpointId)
    //         else
    //           Fail()
    //           recover val Map[StateName, SetIs[Key] val] end
    //         end
    //       end

    //     if t.is_empty() then
    //       @printf[I32]("----This worker has no steps----\n".cstring())
    //     end

    //     let graph = t.graph()

    //     @printf[I32]("Creating graph:\n".cstring())
    //     @printf[I32]((graph.string() + "\n").cstring())

    //     // Make sure we only create shared state once and reuse it
    //     let state_map: Map[StateName, Router] = state_map.create()

    //     @printf[I32](("\nInitializing " + t.name() +
    //       " application locally:\n\n").cstring())

    //     // For passing into partition builders so they can add state steps
    //     // to our data routes
    //     let data_routes_ref = Map[U128, Consumer]

    //     // Keep track of all Consumers by id so we can create a
    //     // DataRouter for the data channel boundary
    //     var data_routes = recover trn Map[U128, Consumer] end

    //     // Update the step ids for all OutgoingBoundaries
    //     if worker_count > 1 then
    //       _connections.update_boundary_ids(t.boundary_ids())
    //     end

    //     // Keep track of routers to the steps we've built
    //     let built_routers = Map[U128, Router]

    //     // Keep track of all stateless partition routers we've built
    //     let stateless_partition_routers = Map[U128, StatelessPartitionRouter]

    //     let built_stateless_steps = recover trn Map[RoutingId, Consumer] end

    //     let built_state_steps = Map[String, Array[Step] val]
    //     let built_state_step_ids = Map[String, Map[RoutingId, Step] val]

    //     // Keep track of routes we can actually use for messages arriving at
    //     // state steps (this depends on the state steps' upstreams across
    //     // pipelines). Map from state name to router.
    //     let state_step_routers = Map[String, TargetIdRouter]

    //     // If this worker has at least one Source, then we'll also need a
    //     // a BarrierSource to ensure that checkpoint barriers always get to
    //     // source targets (even if our local Sources pop out of existence
    //     // for some reason, as when TCPSources disconnect).
    //     var barrier_source: (BarrierSource | None) = None

    //     /////////
    //     // Initialize based on DAG
    //     //
    //     // Assumptions:
    //     //   I. Acylic graph
    //     /////////

    //     let frontier = Array[DagNode[StepInitializer] val]

    //     /////////
    //     // 1. Find graph sinks and add to frontier queue.
    //     //    We'll work our way backwards.
    //     @printf[I32]("Adding sink nodes to frontier\n".cstring())

    //     // Hold non_partitions until the end because we need to build state
    //     // comp targets first. (Holding to the end means processing first,
    //     // since we're pushing onto a stack). On the other hand, we
    //     // put all source data nodes on the bottom of the stack since
    //     // sources should be processed last.
    //     let non_partitions = Array[DagNode[StepInitializer] val]
    //     let source_data_nodes = Array[DagNode[StepInitializer] val]
    //     for node in graph.nodes() do
    //       match node.value
    //       | let sd: SourceData =>
    //         source_data_nodes.push(node)
    //       else
    //         if node.is_sink() and node.value.is_prestate() then
    //           @printf[I32](("Adding " + node.value.name() +
    //             " node to frontier\n").cstring())
    //           frontier.push(node)
    //         else
    //           non_partitions.push(node)
    //         end
    //       end
    //     end

    //     for node in non_partitions.values() do
    //       @printf[I32](("Adding " + node.value.name() + " node to frontier\n")
    //         .cstring())
    //       frontier.push(node)
    //     end

    //     for node in source_data_nodes.values() do
    //       @printf[I32](("Adding " + node.value.name() +
    //         " node to end of frontier\n").cstring())
    //       frontier.unshift(node)
    //     end

    //     /////////
    //     // 2. Loop: Check next frontier item for if all outgoing steps have
    //     //          been created
    //     //       if no, send to bottom of frontier stack.
    //     //       if yes, add ins to frontier stack, then build the step
    //     //         (connecting it to its out steps, which have already been
    //     //         built)
    //     // If there are no cycles (I), this will terminate
    //     while frontier.size() > 0 do
    //       let next_node =
    //         try
    //           frontier.pop()?
    //         else
    //           @printf[I32](("Graph frontier stack was empty when node was " +
    //             "still expected\n").cstring())
    //           error
    //         end

    //       if built_routers.contains(next_node.id) then
    //         // We've already handled this node (probably because it's
    //         // pre-state)
    //         // TODO: I don't think this should ever happen.
    //         @printf[I32](("We've already handled " + next_node.value.name() +
    //           " with id " + next_node.id.string() + " so we're not handling " +
    //           " it again\n").cstring())
    //         continue
    //       end

    //       // We are only ready to build a node if all of its outputs
    //       // have been built
    //       if _is_ready_for_building(next_node, built_routers) then
    //         @printf[I32](("Handling " + next_node.value.name() + " node\n")
    //           .cstring())
    //         let next_initializer: StepInitializer = next_node.value

    //         // ...match kind of initializer and go from there...
    //         match next_initializer
    //         | let builder: StepBuilder =>
    //         ///////////////
    //         // STEP BUILDER
    //         ///////////////
    //           let next_id = builder.id()
    //           @printf[I32](("Handling id " + next_id.string() + "\n")
    //             .cstring())

    //           if builder.is_prestate() then
    //           ///////////////////
    //           // PRESTATE BUILDER
    //             @printf[I32](("----Spinning up " + builder.name() + "----\n")
    //               .cstring())

    //             ////
    //             // Create the state partition if it doesn't exist
    //             if builder.state_name() != "" then
    //               try
    //                 let state_name = builder.state_name()
    //                 state_step_routers.insert_if_absent(state_name,
    //                   StateStepRouter.from_boundaries(_worker_name,
    //                     _outgoing_boundaries))?
    //                 t.update_state_map(state_name, state_map,
    //                   _metrics_conn, _event_log, local_keys,
    //                   _recovery_replayer, _auth,
    //                   _outgoing_boundaries, _initializables,
    //                   data_routes_ref, built_state_steps,
    //                   built_state_step_ids, _router_registry)?
    //               else
    //                 @printf[I32]("Failed to update state_map\n".cstring())
    //                 error
    //               end
    //             end

    //             let partition_router =
    //               try
    //                 builder.clone_router_and_set_input_type(
    //                   state_map(builder.state_name())?)
    //               else
    //                 // Not a partition, so we need a direct target router
    //                 @printf[I32](("No partition router found for " +
    //                   builder.state_name() + "\n").cstring())
    //                 error
    //               end

    //             let state_comp_target_router =
    //               if builder.pre_state_target_ids().size() > 0 then
    //                 let routers = recover iso Array[Router] end
    //                 for id in builder.pre_state_target_ids().values() do
    //                   try
    //                     routers.push(builder.clone_router_and_set_input_type(
    //                       built_routers(id)?))
    //                   else
    //                     @printf[I32]("No router found to prestate target\n"
    //                       .cstring())
    //                     error
    //                   end
    //                 end
    //                 ifdef debug then
    //                   Invariant(routers.size() > 0)
    //                 end
    //                 if routers.size() == 1 then
    //                   routers(0)?
    //                 else
    //                   MultiRouter(consume routers)
    //                 end
    //               else
    //                 // This prestate has no computation targets
    //                 EmptyRouter
    //               end

    //             let next_step = builder(partition_router, _metrics_conn,
    //               _event_log, _recovery_replayer, _auth, _outgoing_boundaries,
    //               _router_registry, state_comp_target_router)
    //             _router_registry.register_partition_router_subscriber(
    //               builder.state_name(), next_step)
    //             _router_registry.register_producer(next_id, next_step)

    //             data_routes(next_id) = next_step
    //             _initializables.set(next_step)

    //             built_stateless_steps(next_id) = next_step
    //             let next_router = DirectRouter(next_id, next_step)
    //             built_routers(next_id) = next_router
    //           elseif not builder.is_stateful() then
    //           //////////////////////////////////
    //           // STATELESS, NON-PRESTATE BUILDER
    //             @printf[I32](("----Spinning up " + builder.name() + "----\n")
    //               .cstring())
    //             let out_ids: Array[RoutingId] val =
    //               try
    //                 _get_output_node_ids(next_node)?
    //               else
    //                 @printf[I32]("Failed to get output node id\n".cstring())
    //                 error
    //               end

    //             let out_router =
    //               if out_ids.size() > 0 then
    //                 let routers = recover iso Array[Router] end
    //                 for id in out_ids.values() do
    //                   try
    //                     routers.push(builder.clone_router_and_set_input_type(
    //                       built_routers(id)?))
    //                   else
    //                     @printf[I32]("No router found to target\n".cstring())
    //                     error
    //                   end
    //                 end
    //                 ifdef debug then
    //                   Invariant(routers.size() > 0)
    //                 end
    //                 if routers.size() == 1 then
    //                   routers(0)?
    //                 else
    //                   MultiRouter(consume routers)
    //                 end
    //               else
    //                 // This prestate has no computation targets
    //                 EmptyRouter
    //               end

    //             let next_step = builder(out_router, _metrics_conn, _event_log,
    //               _recovery_replayer, _auth, _outgoing_boundaries,
    //               _router_registry)

    //             // ASSUMPTION: If an out_router is a MultiRouter, then none
    //             // of its subrouters are partition routers. Put differently,
    //             // we assume that splits never include partition routers.
    //             match out_router
    //             | let pr: StatelessPartitionRouter =>
    //               _router_registry
    //                 .register_stateless_partition_router_subscriber(
    //                   pr.partition_id(), next_step)
    //             end

    //             data_routes(next_id) = next_step
    //             _initializables.set(next_step)

    //             built_stateless_steps(next_id) = next_step
    //             let next_router = DirectRouter(next_id, next_step)
    //             built_routers(next_id) = next_router
    //           else
    //           ////////////////////////////////
    //           // NON-PARTITIONED STATE BUILDER
    //             // Our step is stateful and non-partitioned, so we need to
    //             // build both a state step and a prestate step

    //             // First, we must check that all state computation targets
    //             // have been built.  If they haven't, then we send this node
    //             // to the back of the frontier queue.
    //             var targets_ready = true
    //             for in_node in next_node.ins() do
    //               for id in in_node.value.pre_state_target_ids().values() do
    //                 try
    //                   built_routers(id)?
    //                 else
    //                   targets_ready = false
    //                 end
    //               end
    //             end

    //             if not targets_ready then
    //               frontier.unshift(next_node)
    //               continue
    //             end

    //             @printf[I32](("----Spinning up state for " + builder.name() +
    //               "----\n").cstring())
    //             let state_step = builder(EmptyRouter, _metrics_conn,
    //               _event_log, _recovery_replayer, _auth, _outgoing_boundaries,
    //               _router_registry)
    //             data_routes(next_id) = state_step
    //             _initializables.set(state_step)

    //             let state_step_router = DirectRouter(next_id, state_step)
    //             built_routers(next_id) = state_step_router

    //             // Before a non-partitioned state builder, we should
    //             // always have one or more non-partitioned pre-state builders.
    //             // The only inputs coming into a state builder should be
    //             // prestate builder, so we're going to build them all
    //             for in_node in next_node.ins() do
    //               match in_node.value
    //               | let b: StepBuilder =>
    //                 @printf[I32](("----Spinning up " + b.name() + "----\n")
    //                   .cstring())

    //                 let state_comp_target =
    //                   if b.pre_state_target_ids().size() > 0 then
    //                     let routers = recover iso Array[Router] end
    //                     for id in b.pre_state_target_ids().values() do
    //                       routers.push(built_routers(id)?)
    //                     else
    //                       @printf[I32](("Prestate comp target not built! We " +
    //                         "should have already caught this\n").cstring())
    //                       error
    //                     end
    //                     ifdef debug then
    //                       Invariant(routers.size() > 0)
    //                     end
    //                     if routers.size() == 1 then
    //                       routers(0)?
    //                     else
    //                       MultiRouter(consume routers)
    //                     end
    //                   else
    //                     @printf[I32](("There is no prestate comp target. " +
    //                       "using an EmptyRouter\n").cstring())
    //                     EmptyRouter
    //                   end

    //                 let pre_state_step = b(state_step_router, _metrics_conn,
    //                   _event_log, _recovery_replayer, _auth,
    //                   _outgoing_boundaries, _router_registry,
    //                   state_comp_target)
    //                 _router_registry.register_producer(b.id(), pre_state_step)
    //                 data_routes(b.id()) = pre_state_step
    //                 _initializables.set(pre_state_step)

    //                 built_stateless_steps(b.id()) = pre_state_step
    //                 let pre_state_router = DirectRouter(b.id(), pre_state_step)
    //                 built_routers(b.id()) = pre_state_router

    //                 let state_name = b.state_name()
    //                 if state_name == "" then
    //                   Fail()
    //                 else
    //                   try
    //                     var ssr = state_step_routers(state_name)?
    //                     match state_comp_target
    //                     | let spr: StatelessPartitionRouter =>
    //                       ssr = ssr.update_stateless_partition_router(
    //                         spr.partition_id(), spr)
    //                     else
    //                       for (r_id, c) in state_comp_target.routes().pairs()
    //                       do
    //                         ssr = ssr.add_consumer(r_id, c)
    //                       end
    //                     end
    //                     state_step_routers(state_name) = ssr
    //                   else
    //                     Fail()
    //                   end
    //                 end

    //                 // Add ins to this prestate node to the frontier
    //                 for in_in_node in in_node.ins() do
    //                   if not built_routers.contains(in_in_node.id) then
    //                     frontier.push(in_in_node)
    //                   end
    //                 end

    //                 @printf[I32](("Finished handling " + in_node.value.name() +
    //                   " node\n").cstring())
    //               else
    //                 @printf[I32](("State steps should only have prestate " +
    //                   "predecessors!\n").cstring())
    //                 error
    //               end
    //             end
    //           end
    //         | let egress_builder: EgressBuilder =>
    //         ////////////////////////////////////
    //         // EGRESS BUILDER (Sink or Boundary)
    //         ////////////////////////////////////
    //           let next_id = egress_builder.id()
    //           if not built_routers.contains(next_id) then
    //             let sink_reporter = MetricsReporter(t.name(),
    //               t.worker_name(), _metrics_conn)

    //             // Create a sink or OutgoingBoundary proxy. If the latter,
    //             // egress_builder finds it from _outgoing_boundaries
    //             let sink =
    //               try
    //                 egress_builder(_worker_name, consume sink_reporter,
    //                   _event_log, _recovering, _barrier_initiator,
    //                   _checkpoint_initiator, _env, _auth, _outgoing_boundaries)?
    //               else
    //                 @printf[I32]("Failed to build sink from egress_builder\n"
    //                   .cstring())
    //                 error
    //               end

    //             match sink
    //             | let d: DisposableActor =>
    //               _connections.register_disposable(d)
    //             else
    //               @printf[I32](("All sinks and boundaries should be " +
    //                 "disposable!\n").cstring())
    //               Fail()
    //             end

    //             if not _initializables.contains(sink) then
    //               _initializables.set(sink)
    //             end

    //             let sink_router =
    //               match sink
    //               | let ob: OutgoingBoundary =>
    //                 match egress_builder.target_address()
    //                 | let pa: ProxyAddress =>
    //                   ProxyRouter(_worker_name, ob, pa, _auth)
    //                 else
    //                   @printf[I32]("No ProxyAddress for proxy!\n".cstring())
    //                   error
    //                 end
    //               else
    //                 built_stateless_steps(next_id) = sink
    //                 DirectRouter(next_id, sink)
    //               end

    //             // Don't add to data_routes unless it's a local sink
    //             match sink
    //             | let ob: OutgoingBoundary => None
    //             else
    //               data_routes(next_id) = sink
    //             end
    //             built_routers(next_id) = sink_router
    //           end
    //         | let pre_stateless_data: PreStatelessData =>
    //         //////////////////////
    //         // PRE-STATELESS DATA
    //         //////////////////////
    //           try
    //             let local_step_ids =
    //               pre_stateless_data.worker_to_step_id(_worker_name)?

    //             // Make sure all local steps for this stateless partition
    //             // have already been initialized on this worker.
    //             var ready = true
    //             for id in local_step_ids.values() do
    //               if not built_stateless_steps.contains(id) then
    //                 ready = false
    //               end
    //             end

    //             if ready then
    //               // Populate partition routes with all local steps in
    //               // the partition and proxy routers for any steps that
    //               // exist on other workers.
    //               let partition_routes =
    //                 recover trn Map[U64, (Step | ProxyRouter)] end

    //               for (p_id, step_id) in
    //                 pre_stateless_data.partition_idx_to_step_id.pairs()
    //               do
    //                 if local_step_ids.contains(step_id) then
    //                   match built_stateless_steps(step_id)?
    //                   | let s: Step =>
    //                     partition_routes(p_id) = s
    //                   else
    //                     @printf[I32](("We should only be creating stateless " +
    //                       "partition routes to Steps!\n").cstring())
    //                     Fail()
    //                   end
    //                 else
    //                   let target_worker =
    //                     pre_stateless_data.partition_idx_to_worker(p_id)?
    //                   let proxy_address = ProxyAddress(target_worker, step_id)

    //                   partition_routes(p_id) = ProxyRouter(target_worker,
    //                     _outgoing_boundaries(target_worker)?, proxy_address,
    //                     _auth)
    //                 end
    //               end

    //               let stateless_partition_router =
    //                 LocalStatelessPartitionRouter(next_node.id, _worker_name,
    //                   pre_stateless_data.partition_idx_to_step_id,
    //                   consume partition_routes,
    //                   pre_stateless_data.steps_per_worker)

    //               built_routers(next_node.id) = stateless_partition_router
    //               stateless_partition_routers(next_node.id) =
    //                 stateless_partition_router
    //             else
    //               // We need to wait until all local stateless partition steps
    //               // are spun up on this worker before we can create the
    //               // LocalStatelessPartitionRouter
    //               frontier.unshift(next_node)
    //             end
    //           else
    //             @printf[I32]("Error spinning up stateless partition\n"
    //               .cstring())
    //             Fail()
    //           end
    //         | let source_data: SourceData =>
    //         /////////////////
    //         // SOURCE DATA
    //         /////////////////
    //           let next_id = source_data.id()
    //           let pipeline_name = source_data.pipeline_name()

    //           ////
    //           // Create the state partition if it doesn't exist
    //           if source_data.state_name() != "" then
    //             try
    //               let state_name = source_data.state_name()
    //               state_step_routers.insert_if_absent(state_name,
    //                 StateStepRouter.from_boundaries(_worker_name,
    //                   _outgoing_boundaries))?
    //               t.update_state_map(state_name, state_map,
    //                 _metrics_conn, _event_log, local_keys,
    //                 _recovery_replayer, _auth,
    //                 _outgoing_boundaries, _initializables,
    //                 data_routes_ref, built_state_steps,
    //                 built_state_step_ids, _router_registry)?
    //             else
    //               @printf[I32]("Failed to update state map\n".cstring())
    //               error
    //             end
    //           end

    //           let state_comp_target_router =
    //             if source_data.is_prestate() then
    //               if source_data.pre_state_target_ids().size() > 0 then
    //                 let routers = recover iso Array[Router] end
    //                 for id in source_data.pre_state_target_ids().values() do
    //                   routers.push(built_routers(id)?)
    //                 else
    //                   @printf[I32](("Prestate comp target not built! We " +
    //                     "should have already caught this\n").cstring())
    //                   error
    //                 end
    //                 ifdef debug then
    //                   Invariant(routers.size() > 0)
    //                 end
    //                 if routers.size() == 1 then
    //                   routers(0)?
    //                 else
    //                   MultiRouter(consume routers)
    //                 end
    //               else
    //                 @printf[I32](("There is no prestate comp target. " +
    //                   "using an EmptyRouter\n").cstring())
    //                 EmptyRouter
    //               end
    //             else
    //               EmptyRouter
    //             end

    //           let out_router =
    //             if source_data.state_name() == "" then
    //               let out_ids = _get_output_node_ids(next_node)?

    //               match out_ids.size()
    //               | 0 => EmptyRouter
    //               | 1 => built_routers(out_ids(0)?)?
    //               else
    //                 let routers = recover iso Array[Router] end
    //                 for id in out_ids.values() do
    //                   routers.push(built_routers(id)?)
    //                 end
    //                 MultiRouter(consume routers)
    //               end
    //             else
    //               // Source has a prestate runner on it, so we have no
    //               // direct target. We need a partition router. And we
    //               // need to register a route to our state comp target on those
    //               // state steps.
    //               try
    //                 source_data.clone_router_and_set_input_type(
    //                   state_map(source_data.state_name())?)
    //               else
    //                 @printf[I32]("State doesn't exist for state computation.\n"
    //                   .cstring())
    //                 error
    //               end
    //             end

    //           // If there is no BarrierSource, we need to create one, since
    //           // this worker has at least one Source on it.
    //           if barrier_source is None then
    //             let b_source = BarrierSource(t.barrier_source_id,
    //               _router_registry, _event_log)
    //             _barrier_initiator.register_barrier_source(b_source)
    //             barrier_source = b_source
    //           end
    //           try
    //             (barrier_source as BarrierSource).register_pipeline(
    //               pipeline_name, out_router)
    //           else
    //             Unreachable()
    //           end

    //           let source_reporter = MetricsReporter(t.name(), t.worker_name(),
    //             _metrics_conn)

    //           let listen_auth = TCPListenAuth(_auth)
    //           @printf[I32](("----Creating source for " + pipeline_name +
    //             " pipeline with " + source_data.name() + "----\n").cstring())

    //           // Set up SourceListener builders
    //           sl_builders.push(source_data.source_listener_builder_builder()(
    //             t.worker_name(), pipeline_name, source_data.runner_builder(),
    //             source_data.grouper(), out_router, _metrics_conn,
    //             consume source_reporter, _router_registry,
    //             _outgoing_boundary_builders, _event_log, _auth, this,
    //             _recovering))

    //           // Nothing connects to a source via an in edge locally,
    //           // so this just marks that we've built this one
    //           built_routers(next_id) = EmptyRouter
    //         end

    //         // Add all the nodes with incoming edges to next_node to the
    //         // frontier
    //         for in_node in next_node.ins() do
    //           if not built_routers.contains(in_node.id) then
    //             frontier.push(in_node)
    //           end
    //         end

    //         @printf[I32](("Finished handling " + next_node.value.name() +
    //           " node\n").cstring())
    //       else
    //         frontier.unshift(next_node)
    //       end
    //     end

    //     //////////////
    //     // Create ProxyRouters to all non-state steps in the
    //     // topology that we haven't yet created routers to
    //     for (tid, target) in t.step_map().pairs() do
    //       if not built_routers.contains(tid) then
    //         match target
    //         | let pa: ProxyAddress val =>
    //           if pa.worker != _worker_name then
    //             built_routers(tid) = ProxyRouter(pa.worker,
    //               _outgoing_boundaries(pa.worker)?, pa, _auth)
    //           end
    //         end
    //       end
    //     end

    //     /////
    //     // Register pre state target routes on corresponding state steps
    //     for psd in t.pre_state_data().values() do
    //       if psd.target_ids().size() > 0 then
    //         // If the corresponding state has not been built yet, build it
    //         // now
    //         if psd.state_name() != "" then
    //           try
    //             let state_name = psd.state_name()
    //             state_step_routers.insert_if_absent(state_name,
    //               StateStepRouter.from_boundaries(_worker_name,
    //                 _outgoing_boundaries))?
    //             t.update_state_map(state_name, state_map,
    //               _metrics_conn, _event_log, local_keys,
    //               _recovery_replayer, _auth,
    //               _outgoing_boundaries, _initializables,
    //               data_routes_ref, built_state_steps,
    //               built_state_step_ids, _router_registry)?
    //           else
    //             @printf[I32]("Failed to update state map\n".cstring())
    //             error
    //           end
    //         end
    //         let partition_router =
    //           try
    //             psd.clone_router_and_set_input_type(state_map(
    //               psd.state_name())?)
    //           else
    //             @printf[I32](("PartitionRouter was not built for expected " +
    //               "state partition.\n").cstring())
    //             error
    //           end
    //         match partition_router
    //         | let pr: PartitionRouter =>
    //           _router_registry.set_partition_router(psd.state_name(), pr)
    //           for tid in psd.target_ids().values() do
    //             let target_router =
    //               try
    //                 built_routers(tid)?
    //               else
    //                 @printf[I32](("Failed to find built router for " +
    //                   "target_router to %s\n").cstring(),
    //                   tid.string().cstring())
    //                 error
    //               end

    //             let state_name = psd.state_name()
    //             if state_name == "" then
    //               Fail()
    //             else
    //               try
    //                 var ssr = state_step_routers(state_name)?
    //                 match target_router
    //                 | let spr: StatelessPartitionRouter =>
    //                   ssr = ssr.update_stateless_partition_router(
    //                     spr.partition_id(), spr)
    //                 else
    //                   for (r_id, c) in target_router.routes().pairs() do
    //                     ssr = ssr.add_consumer(r_id, c)
    //                   end
    //                 end
    //                 state_step_routers(state_name) = ssr
    //               else
    //                 Fail()
    //               end
    //             end
    //             @printf[I32](("Registered routes on state steps for " +
    //               psd.pre_state_name() + "\n").cstring())
    //           end
    //         else
    //           @printf[I32](("Expected PartitionRouter but found something " +
    //             "else!\n").cstring())
    //           error
    //         end
    //       end
    //     end
    //     /////

    //     for (id, s) in built_stateless_steps.pairs() do
    //       match s
    //       | let p: Producer =>
    //         _router_registry.register_producer(id, p)
    //       end
    //     end

    //     for (k, v) in data_routes_ref.pairs() do
    //       data_routes(k) = v
    //     end

    //     let sendable_data_routes = consume val data_routes


    //     let state_steps_iso = recover iso Map[StateName, Array[Step] val] end

    //     for (k, v) in built_state_steps.pairs() do
    //       state_steps_iso(k) = v
    //     end

    //     let sendable_state_steps = consume val state_steps_iso

    //     let data_router_state_routing_ids =
    //       recover iso Map[RoutingId, StateName] end

    //     for (s_name, ws) in t.state_routing_ids.pairs() do
    //       for (w, r_id) in ws.pairs() do
    //         if w == _worker_name then
    //           data_router_state_routing_ids(r_id) = s_name
    //         end
    //       end
    //     end

    //     let data_router = DataRouter(_worker_name, sendable_data_routes,
    //       sendable_state_steps, consume data_router_state_routing_ids)
    //     _router_registry.set_data_router(data_router)
    //     _data_receivers.update_data_router(data_router)

    //     let state_runner_builders = recover iso Map[String, RunnerBuilder] end

    //     for (state_name, subpartition) in t.state_builders().pairs() do
    //       state_runner_builders(state_name) = subpartition.runner_builder()
    //     end

    //     //!@ What do we need to remove related to this?
    //     // _state_step_creator.initialize_routes_and_builders(this,
    //     //   keyed_data_routes_ref.clone(), keyed_step_ids_ref.clone(),
    //     //   _recovery_replayer, _outgoing_boundaries,
    //     //   consume state_runner_builders)

    //     if not _is_initializer then
    //       // Inform the initializer that we're done initializing our local
    //       // topology. If this is the initializer worker, we'll inform
    //       // our ClusterInitializer actor once we've spun up the source
    //       // listeners.
    //       let topology_ready_msg =
    //         try
    //           ChannelMsgEncoder.topology_ready(_worker_name, _auth)?
    //         else
    //           @printf[I32]("ChannelMsgEncoder failed\n".cstring())
    //           error
    //         end

    //       if not _recovering then
    //         _connections.send_control("initializer", topology_ready_msg)
    //       end
    //     end

    //     _router_registry.register_boundaries(_outgoing_boundaries,
    //       _outgoing_boundary_builders)

    //     let stateless_partition_routers_trn =
    //       recover trn Map[U128, StatelessPartitionRouter] end
    //     for (id, router) in stateless_partition_routers.pairs() do
    //       stateless_partition_routers_trn(id) = router
    //     end

    //     for (id, pr) in stateless_partition_routers_trn.pairs() do
    //       _router_registry.set_stateless_partition_router(id, pr)
    //     end

    //     _initializables.application_begin_reporting(this)

    //     @printf[I32]("Local topology initialized\n".cstring())
    //     _topology_initialized = true

    //     if _initializables.size() == 0 then
    //       @printf[I32](("Phases I-II skipped (this topology must only have " +
    //         "sources.)\n").cstring())
    //       _application_ready_to_work()
    //     end
    //   else
    //     @printf[I32](("Local Topology Initializer: No local topology to " +
    //       "initialize\n").cstring())
    //   end

    //   @printf[I32]("\n|^|^|^|Finished Initializing Local Topology|^|^|^|\n"
    //     .cstring())
    //   @printf[I32]("---------------------------------------------------------\n".cstring())

    // else
    //   @printf[I32]("Error initializing local topology\n".cstring())
    //   Fail()
    // end

  fun ref _initialize_joining_worker() =>
    @printf[I32]("!@ STARTING JOINING WORKER BUT INITIALIZE IS COMMENTED OUT\n".cstring())

    //!@ TODO: Uncomment all this stuff below!

    // @printf[I32]("---------------------------------------------------------\n"
    //   .cstring())
    // @printf[I32]("|v|v|v|Initializing Joining Worker Local Topology|v|v|v|\n\n"
    //   .cstring())

    // try
    //   let built_routers = Map[RoutingId, Router]
    //   let local_sinks = recover trn Map[RoutingId, Consumer] end
    //   let data_routes_ref = Map[RoutingId, Consumer]
    //   let state_map = Map[StateName, Router]
    //   let built_state_steps = Map[String, Array[Step] val]
    //   let built_state_step_ids = Map[String, Map[RoutingId, Step] val]
    //   let built_stateless_steps = recover trn Map[RoutingId, Consumer] end
    //   let local_keys = recover val Map[StateName, SetIs[Key] val] end

    //   match _topology
    //   | let t: LocalTopology =>
    //     _router_registry.set_pre_state_data(t.pre_state_data())
    //     // Create sinks
    //     for node in t.graph().nodes() do
    //       match node.value
    //       | let egress_builder: EgressBuilder =>
    //         let next_id = egress_builder.id()
    //         if not built_routers.contains(next_id) then
    //           let sink_reporter = MetricsReporter(t.name(),
    //             t.worker_name(), _metrics_conn)

    //           // Create a sink or OutgoingBoundary. If the latter,
    //           // egress_builder finds it from _outgoing_boundaries
    //           let sink = egress_builder(_worker_name,
    //             consume sink_reporter, _event_log, _recovering,
    //             _barrier_initiator, _checkpoint_initiator, _env, _auth,
    //             _outgoing_boundaries)?

    //           _initializables.set(sink)

    //           match sink
    //           | let d: DisposableActor =>
    //             _connections.register_disposable(d)
    //           else
    //             @printf[I32](("All sinks and boundaries should be " +
    //               "disposable!\n").cstring())
    //             Fail()
    //           end

    //           let sink_router =
    //             match sink
    //             | let ob: OutgoingBoundary =>
    //               match egress_builder.target_address()
    //               | let pa: ProxyAddress =>
    //                 ProxyRouter(_worker_name, ob, pa, _auth)
    //               else
    //                 @printf[I32]("No ProxyAddress for proxy!\n".cstring())
    //                 error
    //               end
    //             else
    //               local_sinks(next_id) = sink
    //               built_stateless_steps(next_id) = sink
    //               DirectRouter(next_id, sink)
    //             end

    //           // Don't add to data routes unless it's a local sink
    //           match sink
    //           | let ob: OutgoingBoundary => None
    //           else
    //             data_routes_ref(next_id) = sink
    //           end

    //           built_routers(next_id) = sink_router
    //         end
    //       else
    //         @printf[I32](("Joining worker only currently supports sinks for " +
    //           "initial topology\n").cstring())
    //         Fail()
    //       end
    //     end

    //     // Create State Steps
    //     for state_name in t.state_builders().keys() do
    //       try
    //         t.update_state_map(state_name, state_map,
    //           _metrics_conn, _event_log, local_keys,
    //           _recovery_replayer, _auth,
    //           _outgoing_boundaries, _initializables,
    //           data_routes_ref, built_state_steps,
    //           built_state_step_ids, _router_registry)?
    //       else
    //         @printf[I32]("Failed to update state_map\n".cstring())
    //         error
    //       end
    //     end

    //     let state_routing_ids = recover iso Map[RoutingId, StateName] end
    //     for (s_name, ws) in t.state_routing_ids.pairs() do
    //       for (w, r_id) in ws.pairs() do
    //         if w == _worker_name then
    //           state_routing_ids(r_id) = s_name
    //         end
    //       end
    //     end

    //     let state_steps_iso = recover iso Map[StateName, Array[Step] val] end

    //     for (k, v) in built_state_steps.pairs() do
    //       state_steps_iso(k) = v
    //     end

    //     let sendable_state_steps = consume val state_steps_iso

    //     let state_step_ids_iso =
    //       recover iso Map[StateName, Map[RoutingId, Step] val] end

    //     for (k, v) in built_state_step_ids.pairs() do
    //       state_step_ids_iso(k) = v
    //     end

    //     let sendable_state_step_ids = consume val state_step_ids_iso

    //     for (id, s) in built_stateless_steps.pairs() do
    //       match s
    //       | let p: Producer =>
    //         _router_registry.register_producer(id, p)
    //       end
    //     end

    //     let data_routes = recover iso Map[RoutingId, Consumer] end
    //     for (k, v) in data_routes_ref.pairs() do
    //       data_routes(k) = v
    //     end

    //     // We have not yet been assigned any keys by the cluster at this
    //     // stage, so we use an empty map to represent that.
    //     let data_router = DataRouter(_worker_name, consume data_routes,
    //       sendable_state_steps, consume state_routing_ids)

    //     let state_runner_builders = recover iso Map[String, RunnerBuilder] end

    //     for (state_name, subpartition) in t.state_builders().pairs() do
    //       state_runner_builders(state_name) = subpartition.runner_builder()
    //     end

    //     //!@ What should we remove related to this?
    //     // _state_step_creator.initialize_routes_and_builders(this,
    //     //   recover LocalStatePartitions end, recover LocalStatePartitionIds end,
    //     //   _recovery_replayer, _outgoing_boundaries, consume state_runner_builders)

    //     _router_registry.set_data_router(data_router)

    //     _router_registry.register_boundaries(_outgoing_boundaries,
    //       _outgoing_boundary_builders)

    //     // Create router blueprints and register state steps to receive
    //     // TargetIdRouters.
    //     _connections.create_routers_from_blueprints(
    //       t.worker_names,
    //       _partition_router_blueprints,
    //       _stateless_partition_router_blueprints, consume local_sinks,
    //       sendable_state_steps, sendable_state_step_ids, _router_registry,
    //       this)

    //     _save_local_topology()
    //     _save_worker_names()

    //     _topology_initialized = true

    //     @printf[I32](("\n|^|^|^|Finished Initializing Joining Worker Local " +
    //       "Topology|^|^|^|\n").cstring())
    //     @printf[I32](("-----------------------------------------------------" +
    //       "----\n").cstring())

    //     @printf[I32]("***Successfully joined cluster!***\n".cstring())
    //   else
    //     Fail()
    //   end
    // else
    //   Fail()
    // end

  //!@ What should happen here?
  be receive_immigrant_key(msg: KeyMigrationMsg) =>
    // try
      match _topology
      | let t: LocalTopology =>
        //!@ What do we do here now?
        None

        // let subpartition = t.state_builders()(msg.state_name())?
        // let runner_builder = subpartition.runner_builder()
        // let reporter = MetricsReporter(t.name(), t.worker_name(),
        //   _metrics_conn)
        // _router_registry.receive_immigrant_key(subpartition, runner_builder,
        //   consume reporter, _recovery_replayer, msg)
      else
        Fail()
      end
    // else
    //   Fail()
    // end

  be ack_migration_batch_complete(sender: String) =>
    _router_registry.ack_migration_batch_complete(sender)

  be shrinkable_query(conn: TCPConnection) =>
    let available = recover iso Array[String] end
    match _topology
    | let t: LocalTopology =>
      for w in t.worker_names.values() do
        if not SetHelpers[String].contains[String](t.non_shrinkable, w) then
          available.push(w)
        end
      end
      let size = available.size()
      let query_reply = ExternalMsgEncoder.shrink_query_response(
        consume available, size.u64())
      conn.writev(query_reply)
    else
      Fail()
    end

  be partition_query(conn: TCPConnection) =>
    _router_registry.partition_query(conn)

  be partition_count_query(conn: TCPConnection) =>
    _router_registry.partition_count_query(conn)

  be cluster_status_query(conn: TCPConnection) =>
    if not _topology_initialized then
      _router_registry.cluster_status_query_not_initialized(conn)
    else
      match _topology
      | let t: LocalTopology =>
        _router_registry.cluster_status_query(t.worker_names, conn)
      else
        Fail()
      end
    end

  be source_ids_query(conn: TCPConnection) =>
    _router_registry.source_ids_query(conn)

  be state_entity_query(conn: TCPConnection) =>
    _router_registry.state_entity_query(conn)

  be stateless_partition_query(conn: TCPConnection) =>
    _router_registry.stateless_partition_query(conn)

  be state_entity_count_query(conn: TCPConnection) =>
    _router_registry.state_entity_count_query(conn)

  be stateless_partition_count_query(conn: TCPConnection) =>
    _router_registry.stateless_partition_count_query(conn)

  be report_status(code: ReportStatusCode) =>
    match code
    | BoundaryCountStatus =>
      @printf[I32]("LocalTopologyInitializer knows about %s boundaries\n"
        .cstring(), _outgoing_boundaries.size().string().cstring())
    end
    _router_registry.report_status(code)

  be initialize_join_initializables() =>
    _initialize_join_initializables()

  fun ref _initialize_join_initializables() =>
    // For now we need to keep boundaries out of the initialization
    // lifecycle stages during join. This is because during a join, all
    // data channels are muted, so we are not able to connect
    // over boundaries. This means the boundaries can not
    // report as initialized until the join is complete, but
    // the join can't complete until we say we're initialized.
    _initializables.remove_boundaries()
    _initializables.application_begin_reporting(this)
    if _initializables.size() == 0 then
      _complete_initialization_lifecycle()
    end

  be report_created(initializable: Initializable) =>
    if not _created.contains(initializable) then
      _created.set(initializable)
      if _created.size() == _initializables.size() then
        @printf[I32]("|~~ INIT PHASE I: Application is created! ~~|\n"
          .cstring())
        _spin_up_source_listeners()
        _initializables.application_created(this)
      end
    else
      @printf[I32]("The same Initializable reported being created twice\n"
        .cstring())
      Fail()
    end

  be report_initialized(initializable: Initializable) =>
    if not _initialized.contains(initializable) then
      _initialized.set(initializable)
      if _initialized.size() == _initializables.size() then
        @printf[I32]("|~~ INIT PHASE II: Application is initialized! ~~|\n"
          .cstring())
        _initializables.application_initialized(this)
      end
    else
      @printf[I32]("The same Initializable reported being initialized twice\n"
        .cstring())
      //!@ Bring this back and solve bug
      // Fail()
    end

  be report_ready_to_work(initializable: Initializable) =>
    if not _ready_to_work.contains(initializable) then
      _ready_to_work.set(initializable)
      if (not _initialization_lifecycle_complete) and
        (_ready_to_work.size() == _initializables.size())
      then
        _complete_initialization_lifecycle()
      end
    else
      @printf[I32](("The same Initializable reported being ready to work " +
        "twice\n").cstring())
      Fail()
    end

  fun ref _complete_initialization_lifecycle() =>
    if _recovering then
      match _topology
      | let t: LocalTopology =>
        _recovery.start_recovery(this, t.worker_names)
      else
        Fail()
      end
    else
      _recovery_ready_to_work = true
      _event_log.quick_initialize(this)
    end
    _router_registry.application_ready_to_work()
    if _is_joining then
      match _joining_state_routing_ids
      | let sri: Map[StateName, RoutingId] val =>
        match _joining_stateless_partition_routing_ids
        | let spri: Map[RoutingId, RoutingId] val =>
          // Call this on router registry instead of Connections directly
          // to make sure that other messages on registry queues are
          // processed first
          _router_registry.inform_contacted_worker_of_initialization(sri,
            spri)
        else
          Fail()
        end
      else
        Fail()
      end
    end
    _initialization_lifecycle_complete = true

  be report_event_log_ready_to_work() =>
    _event_log_ready_to_work = true
    // This should only get called after all initializables have reported
    // they are ready to work, at which point we would have told the EventLog
    // to start pipeline logging.
    Invariant(_ready_to_work.size() == _initializables.size())

    if _recovery_ready_to_work then
      _application_ready_to_work()
    end

  be report_recovery_ready_to_work() =>
    _recovery_ready_to_work = true
    if _event_log_ready_to_work then
      _application_ready_to_work()
    end

  fun ref _application_ready_to_work() =>
    @printf[I32]("|~~ INIT PHASE III: Application is ready to work! ~~|\n"
      .cstring())
    _initializables.application_ready_to_work(this)

    if _is_initializer then
      match _cluster_initializer
      | let ci: ClusterInitializer =>
        ci.topology_ready("initializer")
        _is_initializer = false
      else
        @printf[I32](("Need ClusterInitializer to inform that topology is " +
          "ready\n").cstring())
      end
    end

  fun ref _spin_up_source_listeners() =>
    if not _topology_initialized then
      @printf[I32](("ERROR: Tried to spin up source listeners before " +
        "topology was initialized!\n").cstring())
    else
      for builder in sl_builders.values() do
        let sl = builder(_env)
        _router_registry.register_source_listener(sl)
      end
    end

  be worker_join(conn: TCPConnection, worker_name: String,
    worker_count: USize)
  =>
    match _topology
    | let t: LocalTopology =>
      _router_registry.worker_join(conn, worker_name, worker_count,
        t, t.worker_names.size())
    else
      Fail()
    end

  fun _is_ready_for_building(node: DagNode[StepInitializer] val,
    built_routers: Map[U128, Router]): Bool
  =>
    var is_ready = true
    for out in node.outs() do
      if not built_routers.contains(out.id) then is_ready = false end
    end
    is_ready

  fun _get_output_node_ids(node: DagNode[StepInitializer] val):
    Array[RoutingId] val ?
  =>
    // Make sure this is not a sink or proxy node.
    match node.value
    | let eb: EgressBuilder =>
      @printf[I32](("Sinks and Proxies have no output nodes in the local " +
        "graph!\n").cstring())
      error
    end

    var out_ids = recover iso Array[RoutingId] end
    for out in node.outs() do
      out_ids.push(out.id)
    end
    consume out_ids

  be request_new_worker() =>
    try
      (_cluster_manager as ClusterManager).request_new_worker()
    else
      @printf[I32](("Attempting to request a new worker but cluster manager is"
        + " None").cstring())
    end
