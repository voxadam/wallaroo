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

use "wallaroo/core/common"
use "wallaroo/core/metrics"
use "wallaroo/core/routing"


primitive OutputProcessor
  fun apply[Out: Any val](next_runner: Runner, metric_name: String,
    pipeline_time_spent: U64, output: (Out | Array[Out] val), key: Key,
    producer_id: RoutingId, producer: Producer ref, router: Router,
    i_msg_uid: MsgId, frac_ids: FractionalMessageId, computation_end: U64,
    metrics_id: U16, worker_ingress_ts: U64,
    metrics_reporter: MetricsReporter ref): (Bool, U64)
  =>
    """
    Send any outputs of a computation along to the next Runner, returning
    a tuple indicating if the message has finished processing and the last
    metrics-related timestamp.
    """
    match output
    | let o: Out val =>
      next_runner.run[Out](metric_name, pipeline_time_spent, o, key,
        producer_id, producer, router, i_msg_uid, frac_ids,
        computation_end, metrics_id, worker_ingress_ts, metrics_reporter)
    | let os: Array[Out] val =>
      var this_is_finished = true
      var this_last_ts = computation_end

      for (frac_id, o) in os.pairs() do
        let o_frac_ids = match frac_ids
        | None =>
          recover val
            Array[U32].init(frac_id.u32(), 1)
          end
        | let x: Array[U32 val] val =>
          recover val
            let z = Array[U32](x.size() + 1)
            for xi in x.values() do
              z.push(xi)
            end
            z.push(frac_id.u32())
            z
          end
        end

        //!@ Is this using the correct metrics_id?  Or should we be
        // generating a new one here?
        (let f, let ts) = next_runner.run[Out](metric_name,
          pipeline_time_spent, o, key, producer_id, producer, router,
          i_msg_uid, o_frac_ids, computation_end,
          metrics_id, worker_ingress_ts, metrics_reporter)

        // we are sending multiple messages, only mark this message as
        // finished if all are finished
        if (f == false) then
          this_is_finished = false
        end

        this_last_ts = ts
      end
      (this_is_finished, this_last_ts)
    end
