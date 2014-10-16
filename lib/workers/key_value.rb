module Workers
  class KeyValue
    include SideJob::Worker
    register(
        description: 'Outputs key/values from an object.',
        icon: 'key',
        inports: {
            in: { type: 'object', description: 'Input object' },
        },
        outports: {
            '*' => { type: 'all', description: 'Value output by key (port name)' },
        },
    )

    def perform
      keys = outports.map {|port| port.name.to_s}
      for_inputs(:in) do |input|
        input.each_pair do |key, value|
          output(key).write value if keys.include?(key)
        end
      end
    end
  end
end
