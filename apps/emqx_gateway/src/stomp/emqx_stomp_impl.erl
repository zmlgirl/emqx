%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_impl).

-include_lib("emqx_gateway/include/emqx_gateway.hrl").

-behavior(emqx_gateway_impl).

%% APIs
-export([ load/0
        , unload/0
        ]).

-export([ init/1
        , on_insta_create/3
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

-define(TCP_OPTS, [binary, {packet, raw}, {reuseaddr, true}, {nodelay, true}]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec load() -> ok | {error, any()}.
load() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],

    YourOptions = [param1, param2],
    emqx_gateway_registry:load(stomp, RegistryOptions, YourOptions).

-spec unload() -> ok | {error, any()}.
unload() ->
    emqx_gateway_registry:unload(stomp).

init([param1, param2]) ->
    GwState = #{},
    {ok, GwState}.

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_insta_create(_Insta = #{ id := InstaId,
                            rawconf := RawConf
                          }, Ctx, _GwState) ->
    %% Step1. Fold the rawconfs to listeners
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    %% Step2. Start listeners or escokd:specs
    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(InstaId, Ctx, Lis)
                   end, Listeners),
    %% FIXME: How to throw an exception to interrupt the restart logic ?
    %% FIXME: Assign ctx to InstaState
    {ok, ListenerPids, _InstaState = #{ctx => Ctx}}.

on_insta_update(NewInsta, OldInsta, GwInstaState = #{ctx := Ctx}, GwState) ->
    InstaId = maps:get(id, NewInsta),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_insta_destroy(OldInsta, GwInstaState, GwState),
        on_insta_create(NewInsta, Ctx, GwState)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update stomp instance ~s; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [InstaId, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_insta_destroy(_Insta = #{ id := InstaId,
                             rawconf := RawConf
                           }, _GwInstaState, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_rawconf(RawConf),
    lists:foreach(fun(Lis) ->
        stop_listener(InstaId, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(InstaId, Ctx, {Type, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            io:format("Start stomp ~s:~s listener on ~s successfully.~n",
                      [InstaId, Type, ListenOnStr]),
            Pid;
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to start stomp ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(InstaId, Ctx, Type, ListenOn, SocketOpts, Cfg) ->
    Name = name(InstaId, Type),
    NCfg = Cfg#{
             ctx => Ctx,
             frame_mod => emqx_stomp_frame,
             chann_mod => emqx_stomp_channel
            },
    esockd:open(Name, ListenOn, merge_default(SocketOpts),
                {emqx_gateway_conn, start_link, [NCfg]}).

name(InstaId, Type) ->
    list_to_atom(lists:concat([InstaId, ":", Type])).

merge_default(Options) ->
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(?TCP_OPTS, TcpOpts)} | Options1];
        false ->
            [{tcp_options, ?TCP_OPTS} | Options]
    end.

stop_listener(InstaId, {Type, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(InstaId, Type, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> io:format("Stop stomp ~s:~s listener on ~s successfully.~n",
                        [InstaId, Type, ListenOnStr]);
        {error, Reason} ->
            io:format(standard_error,
                      "Failed to stop stomp ~s:~s listener on ~s: ~0p~n",
                      [InstaId, Type, ListenOnStr, Reason]
                     )
    end,
    StopRet.

stop_listener(InstaId, Type, ListenOn, _SocketOpts, _Cfg) ->
    Name = name(InstaId, Type),
    esockd:close(Name, ListenOn).
