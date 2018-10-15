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
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/recovery"
use "wallaroo/ent/router_registry"
use "wallaroo/core/initialization"
use "wallaroo/core/metrics"
use "wallaroo/core/routing"
use "wallaroo/core/sink/tcp_sink"
use "wallaroo/core/source/tcp_source"
use "wallaroo/core/topology"

interface val SourceListenerBuilder
  fun apply(env: Env): SourceListener

interface val SourceListenerBuilderBuilder
  fun apply(worker_name: String, pipeline_name: String,
    runner_builder: RunnerBuilder, router: Router,
    metrics_conn: MetricsSink, metrics_reporter: MetricsReporter iso,
    router_registry: RouterRegistry,
    outgoing_boundary_builders: Map[String, OutgoingBoundaryBuilder] val,
    event_log: EventLog, auth: AmbientAuth,
    layout_initializer: LayoutInitializer,
    recovering: Bool, target_router: Router = EmptyRouter):
    SourceListenerBuilder
