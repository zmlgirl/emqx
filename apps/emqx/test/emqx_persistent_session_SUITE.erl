%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    %% Start Apps
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx], fun set_special_confs/1),
    Config.

set_special_confs(emqx) ->
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_confs(_) ->
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval(_) ->
    Topic = <<"t_connect_session_expiry_interval/foo">>,
    Payload = "test message",

    {ok, Client1} = emqtt:start_link([
                                        {clientid, <<"t_connect_session_expiry_interval">>},
                                        {proto_ver, v5},
                                        {properties, #{'Session-Expiry-Interval' => 7200}}
                                    ]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, qos2),
    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client2),
    {ok, 2} = emqtt:publish(Client2, Topic, Payload, 2),
    ok = emqtt:disconnect(Client2),

    {ok, Client3} = emqtt:start_link([
                                        {clientid, <<"t_connect_session_expiry_interval">>},
                                        {proto_ver, v5},
                                        {properties, #{'Session-Expiry-Interval' => 7200}},
                                        {clean_start, false}
                                    ]),
    {ok, _} = emqtt:connect(Client3),
    [Msg | _ ] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client3).

t_without_client_id(_) ->
    process_flag(trap_exit, true), %% Emqtt client dies
    {ok, Client0} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {properties, #{'Session-Expiry-Interval' => 7200}},
                                        {clean_start, false}
                                    ]),
    {error, {client_identifier_not_valid, _}} = emqtt:connect(Client0),
    ok.

t_assigned_clientid_persistent_session(_) ->
    {ok, Client1} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {properties, #{'Session-Expiry-Interval' => 7200}},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client1),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([
                                        {clientid, AssignedClientId},
                                        {proto_ver, v5},
                                        {clean_start, false}
                                        ]),
    {ok, _} = emqtt:connect(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(_) ->
    ClientId = <<"t_cancel_on_disconnect">>,
    {ok, Client1} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client1),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    {ok, Client2} = emqtt:start_link([
                                        {clientid, ClientId},
                                        {proto_ver, v5},
                                        {clean_start, false}
                                        ]),
    {ok, _} = emqtt:connect(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_process_dies(_) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should survive anyway.
    ClientId = <<"t_process_dies">>,
    {ok, Client1} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client1),
    ok = emqtt:disconnect(Client1),

    [ChannelPid] = emqx_cm:lookup_channels(ClientId),
    ?assert(is_pid(ChannelPid)),
    exit(ChannelPid, kill),

    {ok, Client2} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {clean_start, false}
                                    ]),
    {ok, _} = emqtt:connect(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    emqtt:disconnect(Client2).

t_process_dies_session_expires(_) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ClientId = <<"t_process_dies_session_expires">>,
    {ok, Client1} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {properties, #{'Session-Expiry-Interval' => 1}},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client1),
    ok = emqtt:disconnect(Client1),

    [ChannelPid] = emqx_cm:lookup_channels(ClientId),
    ?assert(is_pid(ChannelPid)),
    exit(ChannelPid, kill),

    timer:sleep(1000),

    {ok, Client2} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {clean_start, false}
                                    ]),
    {ok, _} = emqtt:connect(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    emqtt:disconnect(Client2).

t_clean_start_drops_subscriptions(_) ->
    Topic = <<"t_clean_start_drops_subscriptions/bar">>,
    STopic = <<"t_clean_start_drops_subscriptions/+">>,
    Payload = <<"hello">>,
    ClientId = <<"t_clean_start_drops_subscriptions">>,
    {ok, Client1} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {properties, #{'Session-Expiry-Interval' => 30}},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, [0]} = emqtt:subscribe(Client1, STopic, 0),
    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([
                                        {proto_ver, v5},
                                        {clientid, ClientId},
                                        {clean_start, true}
                                    ]),
    {ok, _} = emqtt:connect(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    {ok, _, [0]} = emqtt:subscribe(Client2, STopic, 0),

    {ok, Client3} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client3),
    {ok, 2} = emqtt:publish(Client3, Topic, Payload, 2),
    ok = emqtt:disconnect(Client3),

    [_Msg1] = receive_messages(1),

    ok = emqtt:disconnect(Client2).

t_subscription_while_process_is_gone(_) ->
    Topic = <<"t_clean_start_drops_subscriptions/bar">>,
    STopic = <<"t_clean_start_drops_subscriptions/+">>,
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = <<"t_clean_start_drops_subscriptions">>,
    {ok, Client1} = emqtt:start_link([
                                      {proto_ver, v5},
                                      {clientid, ClientId},
                                      {properties, #{'Session-Expiry-Interval' => 30}},
                                      {clean_start, true}
                                     ]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, 2),
    ok = emqtt:disconnect(Client1),

    [ChannelPid] = emqx_cm:lookup_channels(ClientId),
    ?assert(is_pid(ChannelPid)),
    exit(ChannelPid, kill),

    {ok, Client2} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Client2),
    {ok, 2} = emqtt:publish(Client2, Topic, Payload1, 2),
    {ok, 3} = emqtt:publish(Client2, Topic, Payload2, 2),

    {ok, Client3} = emqtt:start_link([
                                      {proto_ver, v5},
                                      {clientid, ClientId},
                                      {properties, #{'Session-Expiry-Interval' => 30}},
                                      {clean_start, false}
                                     ]),
    {ok, _} = emqtt:connect(Client3),
    [Msg1] = receive_messages(1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2),
    ok = emqtt:disconnect(Client3).

