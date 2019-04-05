defmodule Events do
  import SweetXml

  def main(_args) do
    events = File.stream!("real.xml")
      |> stream_tags(:ROW) 
      |> Stream.map(fn {_, doc} -> xpath(doc, ~x".",
          id: ~x"./COLUMN[@NAME='ID']/text()"S,
          aggregate_id: ~x"./COLUMN[@NAME='AGGREGATEID']/text()"S,
          source: ~x"./COLUMN[@NAME='SOURCE']/text()"S,
          nature: ~x"./COLUMN[@NAME='NATURE']/text()"S,
          type: ~x"./COLUMN[@NAME='TYPE']/text()"S,
          emitted_at: ~x"./COLUMN[@NAME='EMITTEDAT']/text()"S,
          received_at: ~x"./COLUMN[@NAME='RECEIVEDAT']/text()"S,
          system_offset: ~x"./COLUMN[@NAME='SYSTEMOFFSET']/text()"I,
          payload: ~x"./COLUMN[@NAME='PAYLOAD']/text()"s
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
      |> Stream.flat_map(& &1.contract["salesPoints"])
      |> Stream.flat_map(& &1["terminals"])
      |> Stream.flat_map(& &1["pricing"])
      |> Stream.uniq
      |> Enum.take(3)

      
    #Scribe.print results
    IO.inspect results
    IO.puts results |> Poison.encode |> elem(1) 

    IO.puts "Found events: #{Enum.count(results)}"
    #IO.puts "Total events: #{Enum.count(events)}"

  end

end
