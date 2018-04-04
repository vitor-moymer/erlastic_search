%%% @author Vitor
%%% @copyright (C) 2017, Moymer
%%% @doc
%%% Module to encapsulate business domain user queries on ES
%%% @end
%%% Created :  Mar 2017
-module(elasticsearch).
-define(TODAY_EPOCH_MS, (calendar:datetime_to_gregorian_seconds({date(),{0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})) * 1000).
-define(YESTERDAY_EPOCH_MS, (calendar:datetime_to_gregorian_seconds({date(),{0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) - 86400 ) * 1000).
-define(TOMORROW_EPOCH_MS, (calendar:datetime_to_gregorian_seconds({date(),{0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) + 86400 ) * 1000).
-define(ONEWEEK_AGO_EPOCH_MS, (calendar:datetime_to_gregorian_seconds({date(),{0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) - (7*86400) ) * 1000).
-define(TWOWEEK_AGO_EPOCH_MS, (calendar:datetime_to_gregorian_seconds({date(),{0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) - (14*86400) ) * 1000).
-define(NOW_MS, erlang:system_time(milli_seconds) ).

-export([
	search_result_total/1,
	 search_result_source/1,
	 first_from_result/1,
	 search_result_source_with_total/1,
	 search_unique_result_inner_nested_list/2,
	 get_source/1,
	 get_version/1,
	 get_source_and_version/1,
	 upsert_script/3,
	 upsert_stored_script/3,
	 bucket_aggregation_result/2,
	 scroll_parameter/0,
	 scroll_parameter/1,
	 scroll_id/1,
	 scrolling/2,
	 get_doc_opts_with_version/4,
	 get_doc_and_doc_opts_with_version/4,
	 random_docs/0]).

-include_lib("erlastic_search/include/erlastic_search.hrl").

%%--------------------------------------------------------------------
%% @doc get just object (_source) from ES search result
%% @spec search_result_source(ResultList :: ElasticSearch ResultList)
%% @end
%%--------------------------------------------------------------------

search_result_total(Result) ->
    case Result of 
	{ok,ResultList} ->
	    {<<"hits">>,HitList} = lists:keyfind(<<"hits">>, 1, ResultList),
	    case lists:keyfind(<<"total">>, 1, HitList) of
		{<<"total">>,V} -> V;
		_ -> 0
	    end;
	{error,Error} ->
	    io:format("Search Error: ~p~n",[Error]),
	    0
    end.	
search_result_source(Result) ->
    case Result of 
	{ok,ResultList} ->
	    {<<"hits">>,HitList} = lists:keyfind(<<"hits">>, 1, ResultList),
	    case lists:keyfind(<<"total">>, 1, HitList) of
		{<<"total">>,0} -> [] ;
		_ -> {<<"hits">>,FoundList} = lists:keyfind(<<"hits">>, 1, HitList),
		     [ aux_get_source(Found) || Found <- FoundList]
	    end;
	{error,Error} ->
	    io:format("Search Error: ~p~n",[Error]),
	    []
    end.

first_from_result(Result) ->
    R = search_result_source(Result),
    first(R).
first([]) -> <<>>;
first([F|_]) -> F. 
    

search_unique_result_inner_nested_list(Result, ListName) ->
    case Result of
	{ok,ResultList} ->
	    {<<"hits">>,HitList} = lists:keyfind(<<"hits">>, 1, ResultList),
	    case lists:keyfind(<<"total">>, 1, HitList) of
		{<<"total">>,0} ->
		    [] ;
		{<<"total">>,1} -> 
		    {<<"hits">>,[Hit]} = lists:keyfind(<<"hits">>, 1, HitList),
		    {<<"inner_hits">>,FoundList} = lists:keyfind(<<"inner_hits">>, 1, Hit),
		    {ListName,FoundList2} = lists:keyfind(ListName, 1, FoundList),
		    {<<"hits">>,FoundList3} = lists:keyfind(<<"hits">>, 1, FoundList2),
		    {<<"hits">>,FoundList4} = lists:keyfind(<<"hits">>, 1, FoundList3),
		    [ aux_get_source(Found) || Found <- FoundList4]
	    end;
	{error,Error} ->
            io:format("Search Error: ~p~n",[Error]),
	    []
    end.


search_result_source_with_total(Result) ->
   case Result of
       {ok,ResultList} ->
	   {<<"hits">>,HitList} = lists:keyfind(<<"hits">>, 1, ResultList),
	   case lists:keyfind(<<"total">>, 1, HitList) of
	       {<<"total">>,0} -> 
		   [{<<"total">>,0}, {<<"ans">>,[]}] ;
	       Total  -> 
		   {<<"hits">>,FoundList} = lists:keyfind(<<"hits">>, 1, HitList),
		   Ans = [ aux_get_source(Found) || Found <- FoundList],
		   [Total, {<<"ans">>,Ans}]
	   end;
       {error,Error} ->
	   io:format("Search Error: ~p~n",[Error]),
	   []
   end.

aux_get_source(SourceList) ->
    {<<"_source">>, Source} =  lists:keyfind(<<"_source">>, 1, SourceList),
    Source.

scroll_id(Result) ->
    case Result of
	[] -> <<>>;
	{ok,ResultList} ->
   	    {<<"_scroll_id">>,ScrollId} = lists:keyfind(<<"_scroll_id">>, 1, ResultList),
	    ScrollId;
	{error, Error} ->
	    io:format("Search Error: ~p~n",[Error]),
	    <<>>
    end.


get_source(Result) ->
    case Result of
        {ok,SourceList} ->
	    case lists:keyfind(<<"found">>, 1, SourceList) of
		{<<"found">>,true} -> aux_get_source(SourceList) ;
		_ -> <<"not found">>
	    end;
	{error,{404,_}} -> 
	    <<"not found">>;
	{error,Error} ->
            io:format("Get Error: ~p~n",[Error]),
            <<"not found">>
    end.


get_version(Result) ->
    case Result of
        {ok,SourceList} ->
	    case lists:keyfind(<<"found">>, 1, SourceList) of
                {<<"found">>,true} ->
		    {<<"_version">>,Version} = lists:keyfind(<<"_version">>, 1 , SourceList),
		    Version;
		_ -> -1
	    end;
        {error,{404,_}} -> 
            -1;
        {error,Error} ->
            io:format("Get Error: ~p~n",[Error]),
            -1
    end. 

get_source_and_version(Result) ->
    case Result of
        {ok,SourceList} ->
	    case lists:keyfind(<<"found">>, 1, SourceList) of
                {<<"found">>,true} ->
		    { <<"_version">>, Version } = lists:keyfind(<<"_version">>, 1 , SourceList),
		    { Version, aux_get_source(SourceList) };
		_ ->
		    { -1, <<"not found">> }
            end;
	{error,{404,_}} -> 
	    { -1, <<"not found">> };
	{error,Error} ->
	    io:format("Get Error: ~p~n",[Error]),
	    { -1, <<"not found">> }
	end. 

upsert_script(Inline, Params, undefined) ->
    [{<<"script">>,[{<<"lang">> , <<"groovy">>},{<<"inline">>,Inline},{<<"params">>,Params}]}];

upsert_script(Inline, Params, UpsertDoc) ->
    [{<<"script">>,[{<<"lang">> , <<"groovy">>},{<<"inline">>,Inline},{<<"params">>,Params}]},
     {<<"upsert">>,UpsertDoc} ].


upsert_stored_script(ScriptId, Params, undefined) ->    
    [{<<"script">>,[{<<"lang">> , <<"groovy">>},{<<"stored">>,ScriptId},{<<"params">>,Params}]}];

upsert_stored_script(ScriptId, Params, UpsertDoc) ->
    [{<<"script">>,[{<<"lang">> , <<"groovy">>},{<<"stored">>,ScriptId},{<<"params">>,Params}]},
     {<<"upsert">>,UpsertDoc} ].


bucket_aggregation_result(ResultList, Aggr) ->
    {<<"aggregations">>,Aggregation} = lists:keyfind(<<"aggregations">>, 1, ResultList),
    {Aggr,TopTermsObj} = lists:keyfind(Aggr, 1, Aggregation),
    case lists:keyfind(<<"buckets">>, 1, TopTermsObj) of
        {<<"buckets">>,[]} ->
	    [] ;
	{<<"buckets">>,Items} ->
	    Items
    end.



scrolling(ScrollId,Size) ->
    Query = scroll_parameter(Size) ++ [{<<"scroll_id">>, ScrollId}],
    R = erlastic_search:search_scroll(Query),
    List = search_result_source(R),
    ScrollId = scroll_id(R),
    [{<<"scrollId">>,ScrollId},{<<"ans">>,List}].

scroll_parameter(Size) ->
    Timeout = iolist_to_binary([integer_to_binary(trunc(1.5 * Size)),<<"m">>]),
    [{<<"scroll">>,Timeout}].

scroll_parameter() ->
    [{<<"scroll">>,<<"5m">>}].


get_doc_opts_with_version(Index, Type, Id, Opts) ->
    R = erlastic_search:get_doc(Index, Type, Id),
    case get_version(R) of
	-1 ->
	    %%io:format("New document for ~p ~p ~p~n",[Index, Type, Id]),
	    Opts;
	Version -> 
	    %%io:format("Document exist  ~p ~p ~p with version ~p ~n",[Index, Type, Id,Version]),
	    [{<<"version">>, integer_to_binary(Version)}]
    end.


get_doc_and_doc_opts_with_version(Index, Type, Id, Opts) ->
    R = erlastic_search:get_doc(Index, Type, Id),
    case get_source_and_version(R) of
        {-1,<<"not found">>} ->
	    {[],Opts};
	{Version , Doc } -> 
            %%io:format("Document exist  ~p ~p ~p with version ~p ~n",[Index, Type, Id,Version]),                                                                                                                                                                           
            {Doc, [{<<"version">>, integer_to_binary(Version)}]}
    end.


random_docs() ->
    [{<<"sort">>,
      [{ <<"_script">>,
         [
          {<<"script">>,  <<"Math.random() * 1000">>},
          {<<"type">>,<<"number">>},
          {<<"order">>,<<"desc">>}
         ]
       }]
     }].
