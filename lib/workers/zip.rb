module Workers
  class Zip
    include SideJob::Worker
    register(
        description: 'Zips a list of arrays.',
        icon: 'align-justify',
        inports: {
            in: { type: 'array', description: 'Array of arrays' },
        },
        outports: {
            out: { type: 'array', description: 'Output array' },
        },
    )

    def perform
      for_inputs(:in) do |input|
        first = input.shift
        output(:out).write first.zip(*input)
      end
    end
  end
end
