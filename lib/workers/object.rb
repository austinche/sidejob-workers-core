module Workers
  class Object
    include SideJob::Worker
    register(
        description: 'Makes a JSON object from keys and values. Keys are taken from input port names.',
        icon: 'key',
        inports: {
            '*' => { type: 'all', description: 'Value for key (port name)' },
        },
        outports: {
            out: { type: 'object', description: 'Object with key/value pairs' },
        },
    )

    def perform
      ports = inports.map(&:name)
      for_inputs(*ports) do |*data|
        object = {}
        data.each_with_index do |x, i|
          object[ports[i]] = x
        end
        output(:out).write object
      end
    end
  end
end
