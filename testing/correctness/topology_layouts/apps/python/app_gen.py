#
# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
#


import argparse
import struct

import wallaroo

import components

def application_setup(args):
    # Parse user options
    #parser = argparse.ArgumentParser("Topology Layout Test Generator")
    #parser.add_argument("--partitions", type=int, default=40,
    #                help="Number of partitions for use with internal source")
    #pargs, _ = parser.parse_known_args(args)


    partition = wallaroo.partition(components.partition)
    app_name = "topology test"
    pipe_name = "topology test pipeline"

    ab = wallaroo.ApplicationBuilder(app_name)

    print("Using TCP Source")
    in_host, in_port = wallaroo.tcp_parse_input_addrs(args)[0]
    source = wallaroo.TCPSourceConfig(in_host, in_port, decoder)

    ab.new_pipeline(pipe_name, source)

    # programmatically add computations

    # to
    f = components.Tag(1)
    comp = wallaroo.computation(f.func_name)(f)
    ab.to(comp)

    # to_stateful
    f = components.TagState(1)
    comp = wallaroo.state_computation(f.func_name)(f)
    ab.to_stateful(comp, components.State, f.func_name)

    # onetomany
    f = components.TagToMany(1, num=5)
    comp = wallaroo.computation_multi(f.func_name)(f)
    ab.to(comp)

    # to_state_partition
    f = components.TagState(2)
    comp = wallaroo.state_computation(f.func_name)(f)
    ab.to_state_partition(comp, components.State,
                          f.func_name, partition, [])

    ## to_parallel
    #f = components.Tag(2)
    #comp = wallaroo.computation(f.func_name)(f)
    #ab.to_parallel(comp)

    print("Using TCP Sink")
    out_host, out_port = wallaroo.tcp_parse_output_addrs(args)[0]
    ab.to_sink(wallaroo.TCPSinkConfig(out_host, out_port, encoder))
    return ab.build()


@wallaroo.decoder(header_length=4, length_fmt=">I")
def decoder(bs):
    # Expecting a 64-bit unsigned int in big endian followed by a string
    val, key = struct.unpack(">Q", bs[:8])[0], bs[8:].decode()
    return components.Message(val, key)


@wallaroo.encoder
def encoder(msg):
    s = msg.encode()  # pickled object
    return struct.pack(">I{}s".format(len(s)), len(s), s)
