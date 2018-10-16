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

use "wallaroo_labs/guid"
use "wallaroo/core/common"
use "wallaroo_labs/mort"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/topology"

trait Route
  fun ref application_created()
  fun ref application_initialized(step_type: String)
  fun id(): U64
  fun ref dispose()
  // Return false to indicate queue is full and if producer is a Source, it
  // should mute
  fun ref run[D: Any val](metric_name: String, pipeline_time_spent: U64,
    data: D, key: Key, cfp_id: RoutingId, cfp: Producer ref, msg_uid: MsgId,
    frac_ids: FractionalMessageId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)

  fun ref forward(delivery_msg: ReplayableDeliveryMsg,
    pipeline_time_spent: U64, producer_id: RoutingId, cfp: Producer ref,
    latest_ts: U64, metrics_id: U16, metric_name: String,
    worker_ingress_ts: U64)

  fun register_producer(target_id: RoutingId)
  fun unregister_producer(target_id: RoutingId)

  fun ref report_status(code: ReportStatusCode)

trait RouteLogic
  fun ref application_initialized(step_type: String)
  fun ref dispose()

class _RouteLogic is RouteLogic
  let _step_id: RoutingId
  let _step: Producer ref
  let _consumer: Consumer
  var _step_type: String = ""
  var _route_type: String = ""

  new create(step_id: RoutingId, step: Producer ref, consumer: Consumer,
    r_type: String)
  =>
    _step_id = step_id
    _step = step
    _consumer = consumer
    _route_type = r_type

  fun ref application_initialized(step_type: String) =>
    _step_type = step_type
    _report_ready_to_work()

  fun ref dispose() =>
    None

  fun ref _report_ready_to_work() =>
    match _step
    | let s: Step ref =>
      s.report_route_ready_to_work(this)
    end

class _EmptyRouteLogic is RouteLogic
  fun ref application_initialized(step_type: String) =>
    Fail()
    None

  fun ref dispose() =>
    Fail()
    None

class EmptyRoute is Route
  let _route_id: U64 = 1 + RouteIdGenerator()

  fun ref application_created() =>
    None

  fun ref application_initialized(step_type: String) =>
    None

  fun id(): U64 => _route_id
  fun ref dispose() => None

  fun ref run[D: Any val](metric_name: String, pipeline_time_spent: U64,
    data: D, key: Key, cfp_id: RoutingId, cfp: Producer ref, msg_uid: MsgId,
    frac_ids: FractionalMessageId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)
  =>
    Fail()
    true

  fun ref forward(delivery_msg: ReplayableDeliveryMsg,
    pipeline_time_spent: U64, producer_id: RoutingId, cfp: Producer ref,
    latest_ts: U64, metrics_id: U16, metric_name: String,
    worker_ingress_ts: U64)
  =>
    Fail()
    true

  fun register_producer(target_id: RoutingId) =>
    Fail()

  fun unregister_producer(target_id: RoutingId) =>
    Fail()

  fun ref report_status(code: ReportStatusCode) =>
    Fail()
