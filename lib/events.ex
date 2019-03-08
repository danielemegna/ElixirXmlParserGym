defmodule Events do
  import SweetXml

  def main(_args) do
    events = File.read("events.xml")
      |> elem(1)
      |> xpath(
        ~x"/RESULTS/ROW"l,
          id: ~x"./COLUMN[@NAME='ID']/text()"S,
          aggregate_id: ~x"./COLUMN[@NAME='AGGREGATEID']/text()"S,
          source: ~x"./COLUMN[@NAME='SOURCE']/text()"S,
          nature: ~x"./COLUMN[@NAME='NATURE']/text()"S,
          type: ~x"./COLUMN[@NAME='TYPE']/text()"S,
          emitted_at: ~x"./COLUMN[@NAME='EMITTEDAT']/text()"S,
          received_at: ~x"./COLUMN[@NAME='RECEIVEDAT']/text()"S,
          system_offset: ~x"./COLUMN[@NAME='SYSTEMOFFSET']/text()"I,
          payload: ~x"./COLUMN[@NAME='PAYLOAD']/text()"s
      )

    results = events
      |> Enum.filter(&(&1.aggregate_id == "a0w0N00000AWjZFQA1"))

    IO.puts "Total events: #{Enum.count(events)}"
    IO.puts "Found events: #{Enum.count(results)}"
  end
end
