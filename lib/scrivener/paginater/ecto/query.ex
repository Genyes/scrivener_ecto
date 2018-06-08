defimpl Scrivener.Paginater, for: Ecto.Query do
  import Ecto.Query

  alias Scrivener.{Config, Page}

  @moduledoc false

  @spec paginate(Ecto.Query.t(), Scrivener.Config.t()) :: Scrivener.Page.t()
  def paginate(query, %Config{
        page_size: page_size,
        page_number: page_number,
        module: repo,
        caller: caller,
        options: options
      }) do

    total_entries =
      Keyword.get_lazy(options, :total_entries, fn -> total_entries(query, repo, caller) end)

    total_pages = total_pages(total_entries, page_size)

    %Page{
      page_size: page_size,
      page_number: page_number,
      entries: entries(query, repo, page_number, page_size, caller),
      total_entries: total_entries,
      total_pages: total_pages
    }
  end

  @spec entries(Ecto.Query.t(), Ecto.Repo.t(), non_neg_integer(), non_neg_integer(), any()) :: list() | no_return()
  defp entries(query, repo, page_number, page_size, caller) do
    offset = page_size * (page_number - 1)

    subquery = query
      |> get_ordered_query()
      |> unique_record_ids_subquery()
      |> limit(^page_size)
      |> offset(^offset)

    from([base] in query,
      join: x in subquery(subquery),
        on: x.id == base.id
    )
    |> repo.all(caller: caller)
  end

  defp get_ordered_query(query) do
    if length(get_raw_aliases(query)) > 0,
      do: raise(ArgumentError, message: "Passed query MUST NOT have column aliases defined in fragments!")

    stripped_chainable = query 
      |> exclude(:order_by)
      |> exclude(:select)
      |> exclude(:preload)

    orders = extract_query_order_by_asts(query)

    Enum.reduce(orders, stripped_chainable, fn({direction, col_binding}, chainable) ->
      chainable
      |> chain_order_by_query(direction)
      |> update_query_order_bys(direction, col_binding)
    end)
  end

  # yes, this breaks opacity of Ecto.Query; no, I don't care. --KW
  @spec get_raw_aliases(Ecto.Query.t()) :: [binary()]
  defp get_raw_aliases(%Ecto.Query{select: %Ecto.Query.SelectExpr{expr: exprs}}) when is_list(exprs),
    do: do_get_raw_aliases(exprs)
  defp get_raw_aliases(%Ecto.Query{select: %Ecto.Query.SelectExpr{expr: expr}}) when is_tuple(expr),
    do: do_get_raw_aliases([expr])
  defp get_raw_aliases(%Ecto.Query{}), do: []

  #{{{# internal implementation for get_row_aliases/1
  @spec do_get_raw_aliases([tuple()]) :: [binary()]
  defp do_get_raw_aliases(exprs) do
    exprs
    |> Enum.filter(&do_is_fragment?/1)
    |> Enum.map(&do_extract_alias/1)
  end

  @spec do_is_fragment?({:fragment, [], list()} | any()) :: boolean()
  defp do_is_fragment?({:fragment, [], _}), do: true
  defp do_is_fragment?(_), do: false
  
  @spec do_extract_alias({:fragment, [], keyword()}) :: [binary()]
  defp do_extract_alias({:fragment, [], frag_comps}) do
    {:raw, frag_text} = List.last(frag_comps)
    [aliased_name] = Regex.run(~r{\A\s*[Aa][Ss]\s+(\S+)\s*\z}, frag_text, capture: :all_but_first)
    aliased_name
  end
  #}}}

  defp unique_record_ids_subquery(query) do
    from([base] in query,
      group_by: base.id,
      select: base.id
    )
  end

  defp extract_query_order_by_asts(query) do
    Enum.reduce(query.order_bys, [], fn(order_by, asts) -> asts ++ order_by.expr end)
  end

  defp chain_order_by_query(chainable, :asc) do
    from(t in chainable,
      order_by: fragment("array_agg(? ORDER BY ? ASC)", ^0, ^0) # Placeholder values to be replaced later
    )
  end
  defp chain_order_by_query(chainable, :desc) do
    from(t in chainable,
      order_by: fragment("array_agg(? ORDER BY ? DESC)", ^0, ^0) # Placeholder values to be replaced later
    )
  end

  # Puts the column binding ASTs into the ecto query order by most recently addded
  defp update_query_order_bys(query, direction, col_binding_ast) do
    order_index = length(query.order_bys) - 1
    query
    |> put_in([Access.key(:order_bys), Access.at(order_index), Access.key(:expr), Access.at(0), Access.elem(0)], direction)
    |> put_in([Access.key(:order_bys), Access.at(order_index), Access.key(:expr), Access.at(0), Access.elem(1), Access.elem(2), Access.at(1), Access.elem(1)], col_binding_ast)
    |> put_in([Access.key(:order_bys), Access.at(order_index), Access.key(:expr), Access.at(0), Access.elem(1), Access.elem(2), Access.at(3), Access.elem(1)], col_binding_ast)
    |> put_in([Access.key(:order_bys), Access.at(order_index), Access.key(:params)], [])
  end

  defp total_entries(query, repo, caller) do
    total_entries = query
      |> get_ordered_query()
      |> unique_record_ids_subquery()
      |> count_distinct()
      |> repo.one(caller: caller)

    total_entries || 0
  end

  defp count_distinct(query) do
    query
    |> subquery
    |> select([p], count(p.id))
  end

  defp total_pages(0, _), do: 1

  defp total_pages(total_entries, page_size) do
    (total_entries / page_size) |> Float.ceil() |> round
  end
end
