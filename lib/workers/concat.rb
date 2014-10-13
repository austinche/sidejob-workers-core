module Workers
  class Concat
    include SideJob::Worker
    register(
        description: 'Concatenates arrays from all input ports in alphabetical order by port name.',
        icon: 'list-ol',
        inports: {
            '*' => { type: 'array', description: 'Array to concatenate' },
        },
        outports: {
            out: { type: 'array', description: 'Combined array' },
        },
    )

    def perform
      for_inputs(*inports.map(&:name).sort) do |*data|
        # concatenate all data and generate output
        result = data.each_with_object([]) do |x, result|
          result.concat(x)
        end
        output(:out).write result
      end
    end
  end
end
