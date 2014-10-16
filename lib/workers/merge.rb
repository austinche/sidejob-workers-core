module Workers
  class Merge
    include SideJob::Worker
    register(
        description: 'Waits for data on every input port and merges the input JSON objects in alphabetical order by port name.',
        icon: 'list-ul',
        inports: {
            '*' => { type: 'object', description: 'Input object to be merged' }
        },
        outports: {
            out: { type: 'object', description: 'Merged object' },
        },
    )

    def perform
      for_inputs(*inports.map(&:name).sort) do |*data|
        # merge all data and generate output
        result = data.each_with_object({}) do |x, result|
          result.merge!(x)
        end
        output(:out).write result
      end
    end
  end
end
