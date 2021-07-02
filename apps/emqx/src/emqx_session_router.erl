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

-module(emqx_session_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("ekka/include/ekka.hrl").

-logger_header("[Router]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/2]).

%% Route APIs
-export([ do_add_route/2
        , do_delete_route/2
        , match_routes/1
        ]).

-export([ persist/1
        , delivered/2
        , pending/1
        ]).

-export([print_routes/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-type(group() :: binary()).

-type(dest() :: node() | {group(), node()}).

-define(ROUTE_TAB, emqx_session_route).
-define(SESS_MSG_TAB, emqx_session_msg).
-define(MSG_TAB, emqx_persistent_msg).

%% NOTE: It is important that ?DELIVERED > ?UNDELIVERED because of traversal order
-define(DELIVERED, 1).
-define(UNDELIVERED, 0).
-type pending_tag() :: ?DELIVERED | ?UNDELIVERED.
-record(session_msg, {key      :: {binary(), emqx_guid:guid(), pending_tag()},
                      val = [] :: []}).

-rlog_shard({?PERSISTENT_SESSION_SHARD, ?ROUTE_TAB}).
-rlog_shard({?PERSISTENT_SESSION_SHARD, ?SESS_MSG_TAB}).
-rlog_shard({?PERSISTENT_SESSION_SHARD, ?MSG_TAB}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ROUTE_TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, route},
                {attributes, record_info(fields, route)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    ok = ekka_mnesia:create_table(?SESS_MSG_TAB, [
                {type, ordered_set},
                {ram_copies, [node()]},
                {record_name, session_msg},
                {attributes, record_info(fields, session_msg)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    %% TODO: This should be external
    ok = ekka_mnesia:create_table(?MSG_TAB, [
                {type, set},
                {ram_copies, [node()]},
                {record_name, message},
                {attributes, record_info(fields, message)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ROUTE_TAB, ram_copies),
    ok = ekka_mnesia:copy_table(?SESS_MSG_TAB, ram_copies),
    %% TODO: This should be external
    ok = ekka_mnesia:copy_table(?MSG_TAB, ram_copies).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

-spec(start_link(atom(), pos_integer()) -> startlink_ret()).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], [{hibernate_after, 1000}]).

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec(do_add_route(emqx_topic:topic(), dest()) -> ok | {error, term()}).
do_add_route(Topic, SessionID) when is_binary(Topic) ->
    Route = #route{topic = Topic, dest = SessionID},
    case lists:member(Route, lookup_routes(Topic)) of
        true  -> ok;
        false ->
            case emqx_topic:wildcard(Topic) of
                true  ->
                    Fun = fun emqx_router_utils:insert_trie_route/2,
                    emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route],
                                                  ?PERSISTENT_SESSION_SHARD);
                false ->
                    emqx_router_utils:insert_direct_route(?ROUTE_TAB, Route)
            end
    end.

%% @doc Match routes
-spec(match_routes(emqx_topic:topic()) -> [emqx_types:route()]).
match_routes(Topic) when is_binary(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched ->
            lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.

%% Optimize: routing table will be replicated to all router nodes.
match_trie(Topic) ->
    case emqx_trie:empty_session() of
        true -> [];
        false -> emqx_trie:match_session(Topic)
    end.

-spec(do_delete_route(emqx_topic:topic(), dest()) -> ok | {error, term()}).
do_delete_route(Topic, SessionID) ->
    Route = #route{topic = Topic, dest = SessionID},
    case emqx_topic:wildcard(Topic) of
        true  ->
            Fun = fun emqx_router_utils:delete_trie_route/2,
            emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route], ?PERSISTENT_SESSION_SHARD);
        false ->
            emqx_router_utils:delete_direct_route(?ROUTE_TAB, Route)
    end.

%% @doc Print routes to a topic
-spec(print_routes(emqx_topic:topic()) -> ok).
print_routes(Topic) ->
    lists:foreach(fun(#route{topic = To, dest = SessionID}) ->
                      io:format("~s -> ~s~n", [To, SessionID])
                  end, match_routes(Topic)).

%%--------------------------------------------------------------------
%% Message APIs
%%--------------------------------------------------------------------

persist(Msg) ->
    case emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg) of
        true  -> ok;
        false ->
            case match_routes(emqx_message:topic(Msg)) of
                [] -> ok;
                Routes ->
                    ekka_mnesia:dirty_write(?MSG_TAB, Msg),
                    Fun = fun(Route) -> cast(pick(Route), {persist, Route, Msg}) end,
                    lists:foreach(Fun, Routes)
            end
    end.

delivered(SessionID, MsgIds) ->
    cast(pick(SessionID), {delivered, SessionID, MsgIds}).

pending(SessionID) ->
    call(pick(SessionID), {pending, SessionID}).

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(#route{dest = SessionID}) ->
    gproc_pool:pick_worker(session_router_pool, SessionID);
pick(SessionID) when is_binary(SessionID) ->
    gproc_pool:pick_worker(session_router_pool, SessionID).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({pending, SessionID}, _From, State) ->
    {reply, pending_messages(SessionID), State};
handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({persist, #route{dest = SessionID}, Msg}, State) ->
    Key = {SessionID, emqx_message:id(Msg), ?UNDELIVERED},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
    {noreply, State};
handle_cast({delivered, SessionID, MsgIDs}, State) ->
    Fun = fun(MsgID) ->
                  Key = {SessionID, MsgID, ?DELIVERED},
                  ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key })
          end,
    lists:foreach(Fun, MsgIDs),
    {noreply, State};
handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_routes(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

pending_messages(SessionID) ->
    %% TODO: The reading of messages should be from external DB
    Fun = fun() -> [hd(mnesia:read(?MSG_TAB, MsgId))
                    || MsgId <- pending_messages(SessionID, <<>>, ?DELIVERED, [])]
          end,
    {atomic, Msgs} = ekka_mnesia:ro_transaction(?PERSISTENT_SESSION_SHARD, Fun),
    Msgs.

%% The keys are ordered by
%%     {sessionID(), emqx_guid:guid(), ?DELIVERED | ?UNDELIVERED}
%%  where
%%     emqx_guid:guid() is ordered in ts() and by node()
%%     ?UNDELIVERED < ?DELIVERED
%%
%% We traverse the table until we reach another session.
%% TODO: Garbage collect the delivered messages.
pending_messages(SessionID, PrevMsgId, PrevTag, Acc) ->
    case mnesia:dirty_next(?SESS_MSG_TAB, {SessionID, PrevMsgId, PrevTag}) of
        {S, MsgId, Tag} = Key when S =:= SessionID, MsgId =:= PrevMsgId ->
            Tag =:= ?UNDELIVERED andalso error({assert_fail}, Key),
            pending_messages(SessionID, MsgId, Tag, Acc);
        {S, MsgId, Tag} when S =:= SessionID ->
            case PrevTag of
                ?DELIVERED   -> pending_messages(SessionID, MsgId, Tag, Acc);
                ?UNDELIVERED -> pending_messages(SessionID, MsgId, Tag, [PrevMsgId|Acc])
            end;
        _ -> %% Next sessionID or '$end_of_table'
            case PrevTag of
                ?DELIVERED   -> lists:reverse(Acc);
                ?UNDELIVERED -> lists:reverse([PrevMsgId|Acc])
            end
    end.
