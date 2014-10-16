module Workers
  class Array
    include SideJob::Worker
    register(
        description: 'Makes an array a list of values in alphabetical order of port names.',
        icon: 'list-ol',
        inports: {
            '*' => { type: 'all', description: 'Array element' },
        },
        outports: {
            out: { type: 'array', description: 'Output array' },
        },
    )

    def perform
      ports = inports.map(&:name).sort
      for_inputs(*ports) do |*data|
        output(:out).write data
      end
    end
  end
end
