%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_impl).

-include("include/emqx_gateway.hrl").

-type state() :: map().
-type reason() :: any().

%% @doc
-callback init(Options :: list()) -> {error, reason()} | {ok, GwState :: state()}.

%% @doc
-callback on_insta_create(Insta :: instance(),
                          Ctx :: emqx_gateway_ctx:context(),
                          GwState :: state()
                         )
    -> {error, reason()}
     | {ok, [GwInstaPid :: pid()], GwInstaState :: state()}
     %% TODO: v0.2 The child spec is better for restarting child process
     | {ok, [Childspec :: supervisor:child_spec()], GwInstaState :: state()}.

%% @doc
-callback on_insta_update(NewInsta :: instance(),
                          OldInsta :: instance(),
                          GwInstaState :: state(),
                          GwState :: state())
    -> ok
     | {ok, [GwInstaPid :: pid()], GwInstaState :: state()}
     | {ok, [Childspec :: supervisor:child_spec()], GwInstaState :: state()}
     | {error, reason()}.

%% @doc
-callback on_insta_destroy(Insta :: instance(),
                           GwInstaState :: state(),
                           GwState :: state()) -> ok.
