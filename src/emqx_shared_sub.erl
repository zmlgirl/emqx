%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_shared_sub).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Shared Sub]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% APIs
-export([start_link/0]).

-export([ subscribe/3
        , unsubscribe/3
        ]).

-export([dispatch/3]).

-export([ maybe_ack/1
        , maybe_nack_dropped/1
        , nack_no_connection/1
        , is_ack_required/1
        ]).

%% for testing
-export([subscribers/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([strategy/0]).

-type strategy() :: random
                  | round_robin
                  | sticky
                  | hash %% same as hash_clientid, backward compatible
                  | hash_clientid
                  | hash_message
                  | hash_topic.

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).
-define(SHARED_SUBS, emqx_shared_subscriber).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).
-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(IS_LOCAL_PID(Pid), (is_pid(Pid) andalso node(Pid) =:= node())).
-define(ACK, shared_sub_ack).
-define(NACK(Reason), {shared_sub_nack, Reason}).
-define(NO_ACK, no_ack).

-record(state, {pmon}).

-record(emqx_shared_subscription, {group, topic, subpid}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, emqx_shared_subscription},
                {attributes, record_info(fields, emqx_shared_subscription)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, ram_copies).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(subscribe(emqx_topic:group(), emqx_topic:topic(), pid()) -> ok).
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid}).

-spec(unsubscribe(emqx_topic:group(), emqx_topic:topic(), pid()) -> ok).
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).

record(Group, Topic, SubPid) ->
    #emqx_shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

-spec(dispatch(emqx_topic:group(), emqx_topic:topic(), emqx_types:delivery())
      -> emqx_types:deliver_result()).
dispatch(Group, Topic, Delivery) ->
    ?LOG(info, "GROUP ~p Topic:~p", [Group, Topic]),
    dispatch(Group, Topic, Delivery, _FailedSubs = []).

dispatch(Group, Topic, Delivery = #delivery{message = Msg}, FailedSubs) ->
    #message{from = ClientId, topic = SourceTopic} = Msg,
    case pick(strategy(), ClientId, SourceTopic, Group, Topic, FailedSubs, Msg#message.payload) of
        false ->
            {error, no_subscribers};
        {Type, SubPid} ->
            case do_dispatch(SubPid, Topic, Msg, Type) of
                ok -> {ok, 1};
                {error, _Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Group, Topic, Delivery, [SubPid | FailedSubs])
            end
    end.

-spec(strategy() -> strategy()).
strategy() ->
    emqx:get_env(shared_subscription_strategy, random).

-spec(ack_enabled() -> boolean()).
ack_enabled() ->
    emqx:get_env(shared_dispatch_ack_enabled, false).

do_dispatch(SubPid, Topic, Msg, _Type) when SubPid =:= self() ->
    %% Deadlock otherwise
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
do_dispatch(SubPid, Topic, Msg, Type) ->
    dispatch_per_qos(SubPid, Topic, Msg, Type).

%% return either 'ok' (when everything is fine) or 'error'
dispatch_per_qos(SubPid, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
dispatch_per_qos(SubPid, Topic, Msg, retry) ->
    %% Retry implies all subscribers nack:ed, send again without ack
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
dispatch_per_qos(SubPid, Topic, Msg, fresh) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Topic, Msg);
        false ->
            _ = erlang:send(SubPid, {deliver, Topic, Msg}),
            ok
    end.

dispatch_with_ack(SubPid, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    _ = erlang:send(SubPid, {deliver, Topic, with_ack_ref(Msg, {Sender, Ref})}),
    Timeout = case Msg#message.qos of
                  ?QOS_1 -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS);
                  ?QOS_2 -> infinity
              end,
    try
        receive
            {Ref, ?ACK} ->
                ok;
            {Ref, ?NACK(Reason)} ->
                %% the receive session may nack this message when its queue is full
                {error, Reason};
            {'DOWN', Ref, process, SubPid, Reason} ->
                {error, Reason}
        after
            Timeout ->
                {error, timeout}
        end
    after
        _ = erlang:demonitor(Ref, [flush])
    end.

with_ack_ref(Msg, SenderRef) ->
    emqx_message:set_headers(#{shared_dispatch_ack => SenderRef}, Msg).

without_ack_ref(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?NO_ACK}, Msg).

get_ack_ref(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?NO_ACK).

-spec(is_ack_required(emqx_types:message()) -> boolean()).
is_ack_required(Msg) -> ?NO_ACK =/= get_ack_ref(Msg).

%% @doc Negative ack dropped message due to inflight window or message queue being full.
-spec(maybe_nack_dropped(emqx_types:message()) -> ok).
maybe_nack_dropped(Msg) ->
    case get_ack_ref(Msg) of
        ?NO_ACK -> ok;
        {Sender, Ref} -> nack(Sender, Ref, dropped)
    end.

%% @doc Negative ack message due to connection down.
%% Assuming this function is always called when ack is required
%% i.e is_ack_required returned true.
-spec(nack_no_connection(emqx_types:message()) -> ok).
nack_no_connection(Msg) ->
    {Sender, Ref} = get_ack_ref(Msg),
    nack(Sender, Ref, no_connection).

-spec(nack(pid(), reference(), dropped | no_connection) -> ok).
nack(Sender, Ref, Reason) ->
    erlang:send(Sender, {Ref, ?NACK(Reason)}),
    ok.

-spec(maybe_ack(emqx_types:message()) -> emqx_types:message()).
maybe_ack(Msg) ->
    case get_ack_ref(Msg) of
        ?NO_ACK ->
            Msg;
        {Sender, Ref} ->
            erlang:send(Sender, {Ref, ?ACK}),
            without_ack_ref(Msg)
    end.

pick(sticky, ClientId, SourceTopic, Group, Topic, FailedSubs, Msg) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    case is_active_sub(Sub0, FailedSubs) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            {fresh, Sub0};
        false ->
            %% randomly pick one for the first message
            {Type, Sub} = do_pick(random, ClientId, SourceTopic, Group, Topic, [Sub0 | FailedSubs], Msg),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            {Type, Sub}
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs, Msg) ->
    do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs, Msg).

do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs, Msg) ->
    All = subscribers(Group, Topic),
    case All -- FailedSubs of
        [] when All =:= [] ->
            %% Genuinely no subscriber
            false;
        [] ->
            %% All offline? pick one anyway
            {retry, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, All, Msg)};
        Subs ->
            %% More than one available
            {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs, Msg)}
    end.

pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, [Sub], _Msg) ->
    Sub;
pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs, Msg) ->
    Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, length(Subs), Msg),
    lists:nth(Nth, Subs).

do_pick_subscriber(_Group, _Topic, random, _ClientId, _SourceTopic, Count, _Msg) ->
    rand:uniform(Count);
do_pick_subscriber(Group, Topic, hash, ClientId, SourceTopic, Count, Msg) ->
    %% backward compatible
    do_pick_subscriber(Group, Topic, hash_clientid, ClientId, SourceTopic, Count, Msg);
do_pick_subscriber(_Group, _Topic, hash_clientid, ClientId, _SourceTopic, Count, _Msg) ->
    1 + erlang:phash2(ClientId) rem Count;
do_pick_subscriber(_Group, _Topic, hash_topic, _ClientId, SourceTopic, Count, _Msg) ->
    1 + erlang:phash2(SourceTopic) rem Count;
do_pick_subscriber(_Group, _Topic, hash_message, ClientId, SourceTopic, Count, Msg) ->
    try
        case hash_topic_match(SourceTopic, maps:iterator(topic_mapping())) of
            null ->
                %% 映射失败
                1 + erlang:phash2(ClientId) rem Count;
            JsonPath ->
                case emqx_jsonpath:search(JsonPath, Msg) of
                    undefined ->
                        1 + erlang:phash2(ClientId) rem Count;
                    Term ->
                        1 + erlang:phash2(Term) rem Count
                end
        end
    catch
        error:ErrorReason ->
            ?DEBUG("parse json error ~p ", [ErrorReason]),
            1 + erlang:phash2(ClientId) rem Count
    end;
do_pick_subscriber(Group, Topic, round_robin, _ClientId, _SourceTopic, Count, _Msg) ->
    Rem = case erlang:get({shared_sub_round_robin, Group, Topic}) of
              undefined -> rand:uniform(Count) - 1;
              N -> (N + 1) rem Count
          end,
    _ = erlang:put({shared_sub_round_robin, Group, Topic}, Rem),
    Rem + 1.

topic_mapping() ->
    emqx_json:decode(emqx:get_env(shared_sub_hash_topic_path_mapping, {}), [return_maps]).

hash_topic_match(StringTopic, Iterator) ->
    case maps:next(Iterator) of
        {K, V, NextIter} ->
            case string_match(StringTopic, K) of
                true -> V;
                false -> hash_topic_match(StringTopic, NextIter)
            end;
        none -> null
    end.
string_match(String, Pattern) ->
    case string:find(String, Pattern) of
        nomatch -> false;
        _ -> true
    end.

subscribers(Group, Topic) ->
    ets:select(?TAB, [{{emqx_shared_subscription, Group, Topic, '$1'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
    {atomic, PMon} = mnesia:transaction(fun init_monitors/0),
    ok = emqx_tables:new(?SHARED_SUBS, [protected, bag]),
    ok = emqx_tables:new(?ALIVE_SUBS, [protected, set, {read_concurrency, true}]),
    {ok, update_stats(#state{pmon = PMon})}.

init_monitors() ->
    mnesia:foldl(
      fun(#emqx_shared_subscription{subpid = SubPid}, Mon) ->
          emqx_pmon:monitor(SubPid, Mon)
      end, emqx_pmon:new(), ?TAB).

handle_call({subscribe, Group, Topic, SubPid}, _From, State = #state{pmon = PMon}) ->
    mnesia:dirty_write(?TAB, record(Group, Topic, SubPid)),
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true  -> ok;
        false -> ok = emqx_router:do_add_route(Topic, {Group, node()})
    end,
    ok = maybe_insert_alive_tab(SubPid),
    true = ets:insert(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    {reply, ok, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_call({unsubscribe, Group, Topic, SubPid}, _From, State) ->
    mnesia:dirty_delete_object(?TAB, record(Group, Topic, SubPid)),
    true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    delete_route_if_needed({Group, Topic}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) ->
    #emqx_shared_subscription{subpid = SubPid} = NewRecord,
    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

%% The subscriber may have subscribed multiple topics, so we need to keep monitoring the PID until
%% it `unsubscribed` the last topic.
%% The trick is we don't demonitor the subscriber here, and (after a long time) it will eventually
%% be disconnected.
% handle_info({mnesia_table_event, {delete_object, OldRecord, _}}, State = #state{pmon = PMon}) ->
%     #emqx_shared_subscription{subpid = SubPid} = OldRecord,
%     {noreply, update_stats(State#state{pmon = emqx_pmon:demonitor(SubPid, PMon)})};

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{pmon = PMon}) ->
    ?LOG(info, "Shared subscriber down: ~p", [SubPid]),
    cleanup_down(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:erase(SubPid, PMon)})};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, ?TAB, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% keep track of alive remote pids
maybe_insert_alive_tab(Pid) when ?IS_LOCAL_PID(Pid) -> ok;
maybe_insert_alive_tab(Pid) when is_pid(Pid) -> ets:insert(?ALIVE_SUBS, {Pid}), ok.

cleanup_down(SubPid) ->
    ?IS_LOCAL_PID(SubPid) orelse ets:delete(?ALIVE_SUBS, SubPid),
    lists:foreach(
        fun(Record = #emqx_shared_subscription{topic = Topic, group = Group}) ->
            ok = mnesia:dirty_delete_object(?TAB, Record),
            true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
            delete_route_if_needed({Group, Topic})
        end, mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})).

update_stats(State) ->
    emqx_stats:setstat('subscriptions.shared.count',
                       'subscriptions.shared.max',
                       ets:info(?TAB, size)
                      ),
    State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs) ->
    is_alive_sub(Pid) andalso not lists:member(Pid, FailedSubs).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when ?IS_LOCAL_PID(Pid) ->
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    [] =/= ets:lookup(?ALIVE_SUBS, Pid).

delete_route_if_needed({Group, Topic}) ->
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true -> ok;
        false -> ok = emqx_router:do_delete_route(Topic, {Group, node()})
    end.
