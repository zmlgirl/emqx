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

-module(emqx_authz).
-behaviour(emqx_config_handler).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[AuthZ]").

-export([ register_metrics/0
        , init/0
        , init_rule/1
        , lookup/0
        , update/2
        , authorize/5
        , match/4
        ]).

-export([post_config_update/2, pre_config_update/2]).

-define(CONF_KEY_PATH, [emqx_authz, rules]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTHZ_METRICS).

init() ->
    ok = register_metrics(),
    emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    NRules = [init_rule(Rule) || Rule <- lookup()],
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NRules]}, -1).

lookup() ->
    emqx_config:get(?CONF_KEY_PATH, []).

update(Cmd, Rules) ->
    emqx_config:update(?CONF_KEY_PATH, {Cmd, Rules}).

%% For now we only support re-creating the entire rule list
pre_config_update({head, Rule}, OldConf) when is_map(Rule), is_list(OldConf) ->
    [Rule | OldConf];
pre_config_update({tail, Rule}, OldConf) when is_map(Rule), is_list(OldConf) ->
    OldConf ++ [Rule];
pre_config_update({_, NewConf}, _OldConf) ->
    %% overwrite the entire config!
    case is_list(NewConf) of
        true -> NewConf;
        false -> [NewConf]
    end.

post_config_update(undefined, _OldConf) ->
    %_ = [release_rules(Rule) || Rule <- OldConf],
    ok;
post_config_update(NewRules, _OldConf) ->
    %_ = [release_rules(Rule) || Rule <- OldConf],
    InitedRules = [init_rule(Rule) || Rule <- NewRules],
    Action = find_action_in_hooks(),
    ok = emqx_hooks:del('client.authorize', Action),
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [InitedRules]}, -1),
    ok = emqx_acl_cache:drain_cache().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

create_resource(#{type := DB,
                  config := Config
                 } = Rule) ->
    ResourceID = iolist_to_binary([io_lib:format("~s_~s",[?APP, DB]), "_", integer_to_list(erlang:system_time())]),
    case emqx_resource:create(
            ResourceID,
            list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
            Config)
    of
        {ok, _} ->
            Rule#{resource_id => ResourceID};
        {error, already_created} ->
            Rule#{resource_id => ResourceID};
        {error, Reason} ->
            error({load_config_error, Reason})
    end.

-spec(init_rule(rule()) -> rule()).
init_rule(#{topics := Topics,
            action := Action,
            permission := Permission,
            principal := Principal
         } = Rule) when ?ALLOW_DENY(Permission), ?PUBSUB(Action), is_list(Topics) ->
    NTopics = [compile_topic(Topic) || Topic <- Topics],
    Rule#{principal => compile_principal(Principal),
          topics => NTopics
         };

init_rule(#{principal := Principal,
            type := http,
            config := #{url := Url} = Config
           } = Rule) ->
    NConfig = maps:merge(Config, #{base_url => maps:remove(query, Url)}),
    NRule = create_resource(Rule#{config := NConfig}),
    NRule#{principal => compile_principal(Principal)};

init_rule(#{principal := Principal,
            type := DB
         } = Rule) when DB =:= redis;
                        DB =:= mongo ->
    NRule = create_resource(Rule),
    NRule#{principal => compile_principal(Principal)};

init_rule(#{principal := Principal,
            type := DB,
            sql := SQL
         } = Rule) when DB =:= mysql;
                        DB =:= pgsql ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[?APP, DB])),
    NRule = create_resource(Rule),
    NRule#{principal => compile_principal(Principal),
           sql => Mod:parse_query(SQL)
          }.

compile_principal(all) -> all;
compile_principal(#{username := Username}) ->
    {ok, MP} = re:compile(bin(Username)),
    #{username => MP};
compile_principal(#{clientid := Clientid}) ->
    {ok, MP} = re:compile(bin(Clientid)),
    #{clientid => MP};
compile_principal(#{ipaddress := IpAddress}) ->
    #{ipaddress => esockd_cidr:parse(b2l(IpAddress), true)};
compile_principal(#{'and' := Principals}) when is_list(Principals) ->
    #{'and' => [compile_principal(Principal) || Principal <- Principals]};
compile_principal(#{'or' := Principals}) when is_list(Principals) ->
    #{'or' => [compile_principal(Principal) || Principal <- Principals]}.

compile_topic(<<"eq ", Topic/binary>>) ->
    compile_topic(#{'eq' => Topic});
compile_topic(#{'eq' := Topic}) ->
    #{'eq' => emqx_topic:words(bin(Topic))};
compile_topic(Topic) when is_binary(Topic)->
    Words = emqx_topic:words(bin(Topic)),
    case pattern(Words) of
        true  -> #{pattern => Words};
        false -> Words
    end.

pattern(Words) ->
    lists:member(<<"%u">>, Words) orelse lists:member(<<"%c">>, Words).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

b2l(B) when is_list(B) -> B;
b2l(B) when is_binary(B) -> binary_to_list(B).

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize(emqx_types:clientinfo(), emqx_types:all(), emqx_topic:topic(), emqx_permission_rule:acl_result(), rules())
      -> {stop, allow} | {ok, deny}).
authorize(#{username := Username,
            peerhost := IpAddress
           } = Client, PubSub, Topic, _DefaultResult, Rules) ->
    case do_authorize(Client, PubSub, Topic, Rules) of
        {matched, allow} ->
            ?LOG(info, "Client succeeded authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: allow", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(allow)),
            {stop, allow};
        {matched, deny} ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: deny", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(deny)),
            {stop, deny};
        nomatch ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Reasion: ~p", [Username, IpAddress, Topic, "no-match rule"]),
            {stop, deny}
    end.

do_authorize(Client, PubSub, Topic,
               [Connector = #{principal := Principal,
                              type := DB} | Tail] ) ->
    case match_principal(Client, Principal) of
        true ->
            Mod = list_to_existing_atom(io_lib:format("~s_~s",[emqx_authz, DB])),
            case Mod:authorize(Client, PubSub, Topic, Connector) of
                nomatch -> do_authorize(Client, PubSub, Topic, Tail);
                Matched -> Matched
            end;
        false -> do_authorize(Client, PubSub, Topic, Tail)
    end;
do_authorize(Client, PubSub, Topic,
               [#{permission := Permission} = Rule | Tail]) ->
    case match(Client, PubSub, Topic, Rule) of
        true -> {matched, Permission};
        false -> do_authorize(Client, PubSub, Topic, Tail)
    end;
do_authorize(_Client, _PubSub, _Topic, []) -> nomatch.

match(Client, PubSub, Topic,
      #{principal := Principal,
        topics := TopicFilters,
        action := Action
       }) ->
    match_action(PubSub, Action) andalso
    match_principal(Client, Principal) andalso
    match_topics(Client, Topic, TopicFilters).

match_action(publish, publish) -> true;
match_action(subscribe, subscribe) -> true;
match_action(_, all) -> true;
match_action(_, _) -> false.

match_principal(_, all) -> true;
match_principal(#{username := undefined}, #{username := _MP}) ->
    false;
match_principal(#{username := Username}, #{username := MP}) ->
    case re:run(Username, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_principal(#{clientid := Clientid}, #{clientid := MP}) ->
    case re:run(Clientid, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_principal(#{peerhost := undefined}, #{ipaddress := _CIDR}) ->
    false;
match_principal(#{peerhost := IpAddress}, #{ipaddress := CIDR}) ->
    esockd_cidr:match(IpAddress, CIDR);
match_principal(ClientInfo, #{'and' := Principals}) when is_list(Principals) ->
    lists:foldl(fun(Principal, Permission) ->
                  match_principal(ClientInfo, Principal) andalso Permission
                end, true, Principals);
match_principal(ClientInfo, #{'or' := Principals}) when is_list(Principals) ->
    lists:foldl(fun(Principal, Permission) ->
                  match_principal(ClientInfo, Principal) orelse Permission
                end, false, Principals);
match_principal(_, _) -> false.

match_topics(_ClientInfo, _Topic, []) ->
    false;
match_topics(ClientInfo, Topic, [#{pattern := PatternFilter}|Filters]) ->
    TopicFilter = feed_var(ClientInfo, PatternFilter),
    match_topic(emqx_topic:words(Topic), TopicFilter)
        orelse match_topics(ClientInfo, Topic, Filters);
match_topics(ClientInfo, Topic, [TopicFilter|Filters]) ->
   match_topic(emqx_topic:words(Topic), TopicFilter)
       orelse match_topics(ClientInfo, Topic, Filters).

match_topic(Topic, #{'eq' := TopicFilter}) ->
    Topic == TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqx_topic:match(Topic, TopicFilter).

feed_var(ClientInfo, Pattern) ->
    feed_var(ClientInfo, Pattern, []).
feed_var(_ClientInfo, [], Acc) ->
    lists:reverse(Acc);
feed_var(ClientInfo = #{clientid := undefined}, [<<"%c">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [<<"%c">>|Acc]);
feed_var(ClientInfo = #{clientid := ClientId}, [<<"%c">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [ClientId |Acc]);
feed_var(ClientInfo = #{username := undefined}, [<<"%u">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [<<"%u">>|Acc]);
feed_var(ClientInfo = #{username := Username}, [<<"%u">>|Words], Acc) ->
    feed_var(ClientInfo, Words, [Username|Acc]);
feed_var(ClientInfo, [W|Words], Acc) ->
    feed_var(ClientInfo, Words, [W|Acc]).

