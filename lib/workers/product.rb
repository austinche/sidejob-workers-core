module Workers
  class Product
    include SideJob::Worker
    register(
        description: 'Calculates array product on a list of array.',
        icon: 'times',
        inports: {
            in: { type: 'array', description: 'Array of arrays' },
        },
        outports: {
            out: { type: 'array', description: 'Output array of arrays' },
        },
    )

    def perform
      for_inputs(:in) do |input|
        first = input.shift
        output(:out).write first.product(*input)
      end
    end
  end
end
