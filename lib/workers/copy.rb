module Workers
  class Copy
    include SideJob::Worker
    register(
        description: 'Copies input data 0, 1, or more times to the output',
        icon: 'forward',
        inports: {
            copies: { mode: :memory, default: 1, type: 'integer', description: 'Copies of the data to send to output' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      for_inputs(:copies, :in) do |copies, input|
        copies.times { output(:out).write input }
      end
    end
  end
end
