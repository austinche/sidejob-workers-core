module Workers
  class Split
    include SideJob::Worker
    register(
        description: 'Splits a string into an array.',
        icon: 'unlink',
        inports: {
            sep: { type: 'string', description: 'Separator', default: ',' },
            in: { type: 'string', description: 'Input string' },
        },
        outports: {
            out: { type: 'array', description: 'Output array' },
        },
    )

    def perform
      for_inputs(:sep, :in) do |separator, input|
        output(:out).write input.split(separator)
      end
    end
  end
end
