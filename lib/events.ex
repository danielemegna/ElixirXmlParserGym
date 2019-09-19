defmodule Events do
  import SweetXml

  def main(_args) do
    events = File.stream!("real.xml")
      |> stream_tags(:DATA_RECORD) 
      |> Stream.map(fn {_, doc} -> xpath(doc, ~x".",
          id: ~x"./ID/text()"S,
          aggregate_id: ~x"./AGGREGATEID/text()"S,
          source: ~x"./SOURCE/text()"S,
          nature: ~x"./NATURE/text()"S,
          type: ~x"./TYPE/text()"S,
          emitted_at: ~x"./EMITTEDAT/text()"S,
          received_at: ~x"./RECEIVEDAT/text()"S,
          system_offset: ~x"./SYSTEMOFFSET/text()"I,
          payload: ~x"./PAYLOAD/text()"s
      )end)
      |> Stream.map(fn(event) ->
        emitted_at = event.emitted_at # Regex.named_captures(~r/(?<day>[0-9]{2})-(?<month>[A-Z]{3})-(?<year>[0-9]{2})/, event.emitted_at)
        payload = Poison.decode!(event.payload)

        event
          |> Map.put(:payload, payload)
          |> Map.put(:emitted_at, emitted_at)
          |> Map.put(:contract, payload["contract"]["raw"])
      end)

    results = events 
      #|> Stream.filter(& String.contains?(&1.emitted_at, "JAN"))
      #|> Stream.filter(&(&1.aggregate_id == "a0w0N00000AWjZFQA1"))
      #|> Stream.filter(&(&1.type == "SetupReceivedEvent"))
      |> Stream.filter(& !is_nil(&1.contract))
      |> Stream.filter(& !is_nil(&1.contract["salesPoints"]))
      #|> Stream.map(& [
      #    &1.aggregate_id,
      #    &1.emitted_at,
      #    &1.type,
      #    &1.contract["merchant"]["ONLUSType"]
      #  ]
      #)
      |> Stream.map(& &1.contract)
      #|> Stream.filter(& Enum.count(Enum.at(&1["salesPoints"], 0)["terminals"]) > 1)
      #|> Stream.filter(fn(c) -> c["salesPoints"]
      #  |> Enum.at(0)
      #  |> Map.get("terminals")
      #  |> Enum.any?(fn(t) -> Enum.count(t["enablements"]) > 7 end)
      #end)
      #|> Stream.filter(& Enum.count(Enum.at(&1["salesPoints"], 0)["VAS"]) > 1)
      #|> Stream.filter(& Enum.count(Enum.at(&1["salesPoints"], 0)["acquiring"]["schemes"]) < 3)
      #|> Stream.filter(& Enum.count(Enum.at(&1["salesPoints"], 0)["VAS"]) == 2)
      #|> Stream.flat_map(& &1["salesPoints"])
      #|> Stream.flat_map(& &1["VAS"])
      #|> Stream.filter(& &1["productCode"] == "DCC")
      #|> Stream.filter(& &1["pricing"] |> Enum.count >= 0)
      #|> Stream.flat_map(& &1["acquiring"]["schemes"])
      #|> Stream.filter(& &1["acquirer"] == "AMEX")
      #|> Stream.flat_map(& &1["properties"])
      #|> Stream.uniq
      #|> Stream.filter(fn(c) ->
      #  sp = c["salesPoints"] |> Enum.at(0)
      #  (sp["terminals"] |> Enum.count) == 2
      #end)
      #|> Stream.filter(fn(c) -> c["salesPoints"]
      #  |> Enum.at(0)
      #  |> Map.get("terminals")
      #  |> Enum.any?(fn(t) -> Enum.any?(t["enablements"], fn(e) -> e["code"] == "DCC" end) end)
      #end)
      #|> Stream.drop(10)
      |> Enum.take(1)

      
    #Scribe.print results
    #IO.inspect results
    IO.puts results |> Poison.encode(pretty: true) |> elem(1) 

    IO.puts "Found events: #{Enum.count(results)}"
    #IO.puts "Total events: #{Enum.count(events)}"

  end

end
