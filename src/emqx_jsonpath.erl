%%%-------------------------------------------------------------------
%%% @author zmlgirl
%%% @copyright (C) 2021, <COMPANY>
%%% @doc TODO 这 jsonpath 的解析好像不支持 $. 先这样吧
%%% source -> https://github.com/GeneStevens/jsonpath/blob/master/src/jsonpath.erl
%%% @end
%%% Created : 28. 7月 2021 13:58
%%%-------------------------------------------------------------------
-module(emqx_jsonpath).

-export([search/2, replace/3]).
-export([parse_path/1]).

-include("logger.hrl").

search(Path, Data) when is_binary(Data) ->
    search(Path, jiffy:decode(Data));
search(Path, Data) ->
    search_data(parse_path(Path), Data).

replace(Path, Replace, Data) when is_binary(Data) ->
    replace(Path, Replace, jiffy:decode(Data));
replace(Path, Replace, Data) ->
    replace_data(parse_path(Path), Replace, Data).

replace_data([SearchHead|SearchTail], Replace, Structure) ->
    case Structure of
        {TupleList} ->
            %?DEBUG("tuple list: ~p", [TupleList]),
            { replace_tuple_list([SearchHead|SearchTail], Replace, TupleList) };
        List ->
            %?DEBUG("looks like a list: ~p", [List]),
            replace_list([SearchHead|SearchTail], Replace, List)
    end.

replace_list([SearchHead|SearchTail], Replace, List) ->
    try
        Index = list_to_integer(binary_to_list(SearchHead)) + 1,
        case (Index > length(List)) of
            true ->
                %?DEBUG("Index out of range ~p for list size ~p", [Index, length(List)]),
                undefined;
            false ->
                replace_list([Index|SearchTail], Replace, List, 1, [])
        end
    catch
        _:_ ->
            %?DEBUG("This is not an integer: ~p", [SearchHead]),
            undefined
    end.
replace_list([_SearchHead|_SearchTail], _Replace, [], _Count, Accum) ->
    %?DEBUG("at the end of this list with accum: ~p", [Accum]),
    lists:reverse(Accum);
replace_list([SearchHead|SearchTail], Replace, [Head|Tail], Count, Accum) ->
    %?DEBUG("list: ~p", [Head|Tail]),
    Data = case SearchHead of
               Count ->
                   %?DEBUG("Found index ~p", [Count]),
                   case SearchTail of
                       [] ->
                           Replace;
                       _SearchTail ->
                           %?DEBUG("Not last, so no replacement, but replaceing into: ~p", [Head]),
                           replace_data(SearchTail, Replace, Head)
                   end;
               _SearchHead ->
                   %?DEBUG("Not index ~p", [Count]),
                   Head
           end,
    replace_list([SearchHead|SearchTail], Replace, Tail, Count+1, [Data|Accum]).


replace_tuple_list([SearchHead|SearchTail], Replace, TupleList) ->
    replace_tuple_list([SearchHead|SearchTail], Replace, TupleList, []).
replace_tuple_list([_SearchHead|_SearchTail], _Replace, [], Accum) ->
    %?DEBUG("at the end of this tuple list with accum: ~p", [Accum]),
    lists:reverse(Accum);
replace_tuple_list([SearchHead|SearchTail], Replace, [Head|Tail], Accum) ->
    %?DEBUG("tuple: ~p", [Head]),
    Data = case Head of
               {SearchHead, Value} ->
                   %?DEBUG("Found match for ~p: ~p", [SearchHead, {SearchHead, Value}]),
                   case SearchTail of
                       [] ->
                           {SearchHead, Replace};
                       _SearchTail ->
                           %?DEBUG("Not last, so no replacement, but replaceing into : ~p",[Head]),
                           {SearchHead, replace_data(SearchTail, Replace, Value) }
                   end;
               _Other ->
                   %?DEBUG("No match for ~p: ~p", [SearchHead, Other]),
                   Head
           end,
    %?DEBUG("continue processing tail: ~p", [Tail]),
    replace_tuple_list([SearchHead|SearchTail], Replace, Tail, [Data|Accum]).

search_data([], Data) ->
    Data;
search_data([Head|Tail], Data) ->
%%    ?INFO("Searching for ~p in ~p", [Head,Data]),
    case Head of
        <<>> ->
            search_data(Tail, Data);
        _Other ->
            case Data of
                {_Tuple} ->
%%                    ?INFO("found tuple: ~p", [Tuple]),
                    search_tuple([Head|Tail], Data);
                _List ->
%%                    ?INFO("found list: ~p", [List]),
                    search_list([Head|Tail], Data)
            end
    end.

search_list([Head|Tail], List) ->
%%    ?INFO("list search for ~p in ~p",[Head, List]),
    try
        Index = list_to_integer(binary_to_list(Head)) + 1,
        case (Index > length(List)) of
            true ->
                undefined;
            false ->
                search_data(Tail, lists:nth(Index, List))
        end
    catch
        _:_ ->
%%            ?INFO("that wasn't an integer",[]),
            undefined
    end.

search_tuple([Head|Tail], Tuple) ->
    {TuplePayload} = Tuple,
    case lists:keyfind(Head, 1, TuplePayload) of
        false ->
%%            ?INFO("did not find tuple value for ~p. done.", [Head]),
            undefined;
        {Head,Value} ->
%%            ?INFO("found tuple value for ~p: ~p", [Head,Value]),
            search_data(Tail, Value)
    end.

parse_path(Path) ->
    Split = binary:split(Path, [<<".">>,<<"[">>,<<"]">>], [global]),
    lists:filter(fun(X) -> X =/= <<>> end, Split).
