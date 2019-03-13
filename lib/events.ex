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
        payload = Poison.decode!(event.payload)
        %{ event | payload: payload } |> Map.put(:contract, payload["contract"]["raw"])
      end)

    results = events 
      |> Stream.filter(& !is_nil(&1.contract))
      |> Stream.filter(& !is_nil(&1.contract["salesPoints"]))
      |> Stream.filter(&(&1.aggregate_id == "a0w0N00000AWjZFQA1"))
      #|> Stream.filter(&(&1.type == "SetupReceivedEvent"))
      |> Enum.take(10)
      
    IO.inspect results
      |> Enum.map(fn(e) -> [
        e.aggregate_id,
        e.type
      ] end)

    IO.puts "Found events: #{Enum.count(results)}"
    IO.puts "Total events: #{Enum.count(events)}"

  end
end
