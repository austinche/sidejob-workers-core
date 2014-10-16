module Workers
  class Join
    include SideJob::Worker
    register(
        description: 'Joins an array into a string.',
        icon: 'link',
        inports: {
            sep: { mode: :memory, default: ',', type: 'string', description: 'Separator' },
            in: { type: 'array', description: 'Input array' },
        },
        outports: {
            out: { type: 'string', description: 'Output string' },
        },
    )

    def perform
      for_inputs(:sep, :in) do |separator, input|
        output(:out).write input.join(separator)
      end
    end
  end
end
