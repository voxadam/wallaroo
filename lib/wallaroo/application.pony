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

use "collections"
use "net"
use "wallaroo/core/common"
use "wallaroo/core/grouping"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/sink"
use "wallaroo/core/sink/tcp_sink"
use "wallaroo/core/source"
use "wallaroo/core/source/tcp_source"
use "wallaroo/core/state"
use "wallaroo/core/routing"
use "wallaroo/core/topology"
use "wallaroo/ent/network"
use "wallaroo/ent/recovery"
use "wallaroo_labs/collection_helpers"
use "wallaroo_labs/dag"
use "wallaroo_labs/mort"

primitive Wallaroo
  fun source[In: Any val](pipeline_name: String,
    source_config: TypedSourceConfig[In]): Pipeline[In]
  =>
    //!@ Do we need the pipeline id anymore?
    Pipeline[In].from_source(0, pipeline_name, source_config)

  fun build_application(env: Env, app_name: String,
    pipeline: BasicPipeline val)
  =>
    if pipeline.is_finished() then
      Startup(env, app_name, pipeline)
    else
      FatalUserError("A pipeline must terminate in a sink!")
    end

trait BasicPipeline
  fun name(): String
  fun graph(): this->Dag[Stage]
  fun source_id(): USize
  fun is_finished(): Bool
  fun size(): USize

type Stage is (RunnerBuilder | SinkBuilder | SourceConfig | Shuffle |
  GroupByKey)

class Pipeline[Out: Any val] is BasicPipeline
  let _pipeline_id: USize
  let _name: String

  let _stages: Dag[Stage]
  let _dag_sink_ids: Array[RoutingId]
  var _finished: Bool

  var _last_is_shuffle: Bool
  var _last_is_group_by_key: Bool

  new from_source(p_id: USize, n: String,
    source_config: TypedSourceConfig[Out])
  =>
    _pipeline_id = p_id
    _name = n
    _stages = Dag[Stage]
    _dag_sink_ids = Array[RoutingId]
    _finished = false
    _last_is_shuffle = false
    _last_is_group_by_key = false
    let source_id' = _stages.add_node(source_config)
    _dag_sink_ids.push(source_id')

  new create(p_id: USize, n: String,
    stages: Dag[Stage] = Dag[Stage],
    dag_sink_ids: Array[RoutingId] = Array[RoutingId],
    finished: Bool = false,
    last_is_shuffle: Bool = false,
    last_is_group_by_key: Bool = false)
  =>
    _pipeline_id = p_id
    _name = n
    _stages = stages
    _dag_sink_ids = dag_sink_ids
    _finished = finished
    _last_is_shuffle = last_is_shuffle
    _last_is_group_by_key = last_is_group_by_key

  fun is_finished(): Bool => _finished

  fun ref merge(pipeline: Pipeline[Out]): Pipeline[Out] =>
    if _finished then
      _try_merge_with_finished_pipeline()
    elseif (_last_is_shuffle and not pipeline._last_is_shuffle) or
      (not _last_is_shuffle and pipeline._last_is_shuffle)
    then
      _only_one_is_shuffle()
    elseif (_last_is_group_by_key and not pipeline._last_is_group_by_key) or
      (not _last_is_group_by_key and pipeline._last_is_group_by_key)
    then
      _only_one_is_group_by_key()
    else
      // Successful merge
      try
        _stages.merge(pipeline._stages)?
      else
        // We should have ruled this out through the if branches
        Unreachable()
      end
      _dag_sink_ids.append(pipeline._dag_sink_ids)
      return Pipeline[Out](_pipeline_id, _name, _stages, _dag_sink_ids
        where last_is_shuffle = _last_is_shuffle,
        last_is_group_by_key = _last_is_group_by_key)
    end
    Pipeline[Out](_pipeline_id, _name, _stages, _dag_sink_ids)

  fun ref to[Next: Any val](comp_builder: ComputationBuilder[Out, Next],
    parallelization: USize = 1): Pipeline[Next]
  =>
    if not _finished then
      let runner_builder = ComputationRunnerBuilder[Out, Next](comp_builder,
        parallelization)
      let node_id = _stages.add_node(runner_builder)
      try
        for sink_id in _dag_sink_ids.values() do
          _stages.add_edge(sink_id, node_id)?
        end
      else
        Fail()
      end
      Pipeline[Next](_pipeline_id, _name, _stages, [node_id])
    else
      _try_add_to_finished_pipeline()
      Pipeline[Next](_pipeline_id, _name, _stages, _dag_sink_ids)
    end

  fun ref to_state[Next: Any val, S: State ref](
    s_comp: StateComputation[Out, Next, S] val,
    parallelization: USize = 10): Pipeline[Next]
  =>
    if not _finished then
      let runner_builder = StateRunnerBuilder[Out, Next, S](s_comp,
        parallelization)
      let node_id = _stages.add_node(runner_builder)
      try
        for sink_id in _dag_sink_ids.values() do
          _stages.add_edge(sink_id, node_id)?
        end
      else
        Fail()
      end
      Pipeline[Next](_pipeline_id, _name, _stages, [node_id])
    else
      _try_add_to_finished_pipeline()
      Pipeline[Next](_pipeline_id, _name, _stages, _dag_sink_ids)
    end

    //!@ TODO: What about multiple sinks?
  fun ref to_sink(sink_information: SinkConfig[Out]): Pipeline[Out] =>
    if not _finished then
      let sink_builder = sink_information()
      let node_id = _stages.add_node(sink_builder)
      try
        for dag_sink_id in _dag_sink_ids.values() do
          _stages.add_edge(dag_sink_id, node_id)?
        end
      else
        Fail()
      end
      Pipeline[Out](_pipeline_id, _name, _stages, [node_id]
        where finished = true)
    else
      _try_add_to_finished_pipeline()
      Pipeline[Out](_pipeline_id, _name, _stages, _dag_sink_ids)
    end

  fun ref group_by_key(pf: PartitionFunction[Out]): Pipeline[Out] =>
    if not _finished then
      let node_id = _stages.add_node(TypedGroupByKey[Out](pf))
      try
        for sink_id in _dag_sink_ids.values() do
          _stages.add_edge(sink_id, node_id)?
        end
      else
        Fail()
      end
      Pipeline[Out](_pipeline_id, _name, _stages, [node_id]
        where last_is_group_by_key = true)
    else
      _try_add_to_finished_pipeline()
      Pipeline[Out](_pipeline_id, _name, _stages, _dag_sink_ids)
    end

  fun graph(): this->Dag[Stage] => _stages

  fun source_id(): USize => _pipeline_id

  fun size(): USize => _stages.size()

  fun name(): String => _name

  fun _try_add_to_finished_pipeline() =>
    FatalUserError("You can't add further stages after a sink!")

  fun _try_merge_with_finished_pipeline() =>
    FatalUserError("You can't merge with a terminated pipeline!")

  fun _only_one_is_shuffle() =>
    FatalUserError("A pipeline ending with shuffle can only be merged with another!")

  fun _only_one_is_group_by_key() =>
    FatalUserError("A pipeline ending with group_by_key can only be merged with another!")




// class PipelineBuilder[In: Any val, Out: Any val, Last: Any val]
//   let _a: Application
//   let _p: Pipeline[In, Out]
//   let _pipeline_state_names: Array[String] = _pipeline_state_names.create()

//   new create(a: Application, p: Pipeline[In, Out]) =>
//     _a = a
//     _p = p

//   fun ref to[Next: Any val](
//     comp_builder: ComputationBuilder[Last, Next],
//     id: U128 = 0): PipelineBuilder[In, Out, Next]
//   =>
//     let next_builder = ComputationRunnerBuilder[Last, Next](comp_builder)
//     _p.add_runner_builder(next_builder)
//     PipelineBuilder[In, Out, Next](_a, _p)

//   fun ref to_parallel[Next: Any val](
//     comp_builder: ComputationBuilder[Last, Next],
//     id: U128 = 0): PipelineBuilder[In, Out, Next]
//   =>
//     let next_builder = ComputationRunnerBuilder[Last, Next](
//       comp_builder where parallelized' = true)
//     _p.add_runner_builder(next_builder)
//     PipelineBuilder[In, Out, Next](_a, _p)

//   fun ref to_stateful[Next: Any val, S: State ref](
//     s_comp: StateComputation[Last, Next, S] val,
//     s_initializer: StateBuilder[S],
//     state_name: StateName): PipelineBuilder[In, Out, Next]
//   =>
//     if ArrayHelpers[StateName]
//       .contains[StateName](_pipeline_state_names, state_name)
//     then
//       FatalUserError("Wallaroo does not currently support application " +
//         "cycles. You cannot use the same state name twice in the same " +
//         "pipeline.")
//     end
//     _pipeline_state_names.push(state_name)

//     // TODO: This is a shortcut. Non-partitioned state is being treated as a
//     // special case of partitioned state with one partition. This works but is
//     // a bit confusing when reading the code.
//     let routing_id_gen = RoutingIdGenerator
//     let single_step_partition = Partitions[Last](
//       SingleStepPartitionFunction[Last], recover ["key"] end)
//     let step_id_map = recover trn Map[Key, RoutingId] end

//     step_id_map("key") = routing_id_gen()

//     let next_builder = PreStateRunnerBuilder[Last, Next, S](
//       s_comp, state_name, SingleStepPartitionFunction[Last])

//     _p.add_runner_builder(next_builder)

//     let state_builder = PartitionedStateRunnerBuilder[Last, S](_p.name(),
//       state_name, consume step_id_map, single_step_partition,
//       StateRunnerBuilder[S](s_initializer, state_name,
//         s_comp.state_change_builders()) where per_worker_parallelism' = 1)

//     _a.add_state_builder(state_name, state_builder)

//     PipelineBuilder[In, Out, Next](_a, _p)

//   fun ref to_state_partition[Next: Any val, S: State ref](
//       s_comp: StateComputation[Last, Next, S] val,
//       s_initializer: StateBuilder[S],
//       state_name: StateName,
//       partition: Partitions[Last],
//       multi_worker: Bool = false,
//       per_worker_parallelism: USize = 10): PipelineBuilder[In, Out, Next]
//   =>
//     if ArrayHelpers[StateName]
//       .contains[StateName](_pipeline_state_names, state_name)
//     then
//       FatalUserError("Wallaroo does not currently support application " +
//         "cycles. You cannot use the same state name twice in the same " +
//         "pipeline.")
//     end
//     _pipeline_state_names.push(state_name)

//     let routing_id_gen = RoutingIdGenerator
//     let step_id_map = recover trn Map[Key, RoutingId] end

//     for key in partition.keys().values() do
//       step_id_map(key) = routing_id_gen()
//     end

//     let next_builder = PreStateRunnerBuilder[Last, Next, S](
//       s_comp, state_name, partition.function()
//       where multi_worker = multi_worker)

//     _p.add_runner_builder(next_builder)

//     let state_builder = PartitionedStateRunnerBuilder[Last, S](_p.name(),
//       state_name, consume step_id_map, partition,
//       StateRunnerBuilder[S](s_initializer, state_name,
//         s_comp.state_change_builders()), per_worker_parallelism
//       where multi_worker = multi_worker)

//     _a.add_state_builder(state_name, state_builder)

//     PipelineBuilder[In, Out, Next](_a, _p)

//   fun ref done(): Application =>
//     _a.add_pipeline(_p)
//     _a

//   fun ref to_sink(sink_information: SinkConfig[Out]): Application =>
//     let sink_builder = sink_information()
//     _a.increment_sink_count()
//     _p.add_sink(sink_builder)
//     _a.add_pipeline(_p)
//     _a

//   fun ref to_sinks(sink_configs: Array[SinkConfig[Out]] box): Application =>
//     if sink_configs.size() == 0 then
//       FatalUserError("You must specify at least one sink when using " +
//         "to_sinks()")
//     end
//     for config in sink_configs.values() do
//       let sink_builder = config()
//       _a.increment_sink_count()
//       _p.add_sink(sink_builder)
//     end
//     _a.add_pipeline(_p)
//     _a
