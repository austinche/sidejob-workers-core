module Workers
  class Repeat
    include SideJob::Worker
    register(
        description: 'Outputs an array with a repeated value',
        icon: 'repeat',
        inports: {
            times: { type: 'integer', description: 'Times to repeat the input data' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'array', description: 'Output array with input data repeated' },
        },
    )

    def perform
      for_inputs(:times, :in) do |times, input|
        output(:out).write(::Array.new(times) { input })
      end
    end
  end
end
