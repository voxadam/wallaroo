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

use "wallaroo_labs/mort"
use "wallaroo/core/common"
use "wallaroo/core/invariant"
use "wallaroo/core/topology"
use "wallaroo/ent/barrier"
use "wallaroo/ent/checkpoint"

trait SinkMessageProcessor
  fun ref process_message[D: Any val](metric_name: String,
    pipeline_time_spent: U64, data: D, key: Key, i_producer_id: RoutingId,
    i_producer: Producer, msg_uid: MsgId, frac_ids: FractionalMessageId,
    i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)

  fun barrier_in_progress(): Bool =>
    false

  fun ref receive_new_barrier(step_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    Fail()

  fun ref receive_barrier(step_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    Fail()

  fun ref queued(): Array[_Queued]

class EmptySinkMessageProcessor is SinkMessageProcessor
  fun ref process_message[D: Any val](metric_name: String,
    pipeline_time_spent: U64, data: D, key: Key, i_producer_id: RoutingId,
    i_producer: Producer, msg_uid: MsgId, frac_ids: FractionalMessageId,
    i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)
  =>
    Fail()

  fun ref queued(): Array[_Queued] =>
    Array[_Queued]

class NormalSinkMessageProcessor is SinkMessageProcessor
  let sink: Sink ref

  new create(s: Sink ref) =>
    sink = s

  fun ref process_message[D: Any val](metric_name: String,
    pipeline_time_spent: U64, data: D, key: Key, i_producer_id: RoutingId,
    i_producer: Producer, msg_uid: MsgId, frac_ids: FractionalMessageId,
    i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)
  =>
    sink.process_message[D](metric_name, pipeline_time_spent, data, key,
      i_producer_id, i_producer, msg_uid, frac_ids, i_seq_id, i_route_id,
      latest_ts, metrics_id, worker_ingress_ts)

  fun ref queued(): Array[_Queued] =>
    Array[_Queued]

type _Queued is (QueuedMessage | QueuedBarrier)

class BarrierSinkMessageProcessor is SinkMessageProcessor
  let sink: Sink ref
  let _barrier_acker: BarrierSinkAcker
  let _queued: Array[_Queued] = _queued.create()

  new create(s: Sink ref, barrier_acker: BarrierSinkAcker) =>
    sink = s
    _barrier_acker = barrier_acker

  fun ref process_message[D: Any val](metric_name: String,
    pipeline_time_spent: U64, data: D, key: Key, i_producer_id: RoutingId,
    i_producer: Producer, msg_uid: MsgId, frac_ids: FractionalMessageId,
    i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)
  =>
    if _barrier_acker.input_blocking(i_producer_id) then
      let msg = TypedQueuedMessage[D](metric_name, pipeline_time_spent,
        data, key, i_producer_id, i_producer, msg_uid, frac_ids, i_seq_id,
        i_route_id, latest_ts, metrics_id, worker_ingress_ts)
      _queued.push(msg)
    else
      sink.process_message[D](metric_name, pipeline_time_spent, data, key,
        i_producer_id, i_producer, msg_uid, frac_ids, i_seq_id, i_route_id,
        latest_ts, metrics_id, worker_ingress_ts)
    end

  fun barrier_in_progress(): Bool =>
    true

  fun ref receive_new_barrier(input_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    _barrier_acker.receive_new_barrier(input_id, producer, barrier_token)

  fun ref receive_barrier(input_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    if _barrier_acker.input_blocking(input_id) then
      _queued.push(QueuedBarrier(input_id, producer, barrier_token))
    else
      _barrier_acker.receive_barrier(input_id, producer, barrier_token)
    end

  fun ref queued(): Array[_Queued] =>
    let qd = Array[_Queued]
    for q in _queued.values() do
      qd.push(q)
    end
    qd
