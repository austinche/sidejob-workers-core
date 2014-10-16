module Workers
  class Flatten
    include SideJob::Worker
    register(
        description: 'Flattens an array.',
        icon: 'outdent',
        inports: {
            in: { type: 'array', description: 'Input array' },
            level: { default: nil, type: 'integer', description: 'If specified, provides the level of recursion to flatten' }
        },
        outports: {
            out: { type: 'array', description: 'Output array' },
        },
    )

    def perform
      for_inputs(:in, :level) do |input, level|
        if level
          output(:out).write input.flatten(level)
        else
          output(:out).write input.flatten
        end
      end
    end
  end
end
